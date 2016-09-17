# Mirror the contents of a JCR from one repository to another
#
# To use this script:
#
#   WEBDAV_SRC=http://www.thecollectingbug.com:8080 WEBDAV_DST=http://localhost:8080 PASSWORD=pw ROOT=/jcrpath ruby test.rb

require 'active_record'
require 'net/http'
require 'net/http/post/multipart'
require 'awesome_print'
require 'json'
require 'addressable/uri'

require_relative 'app/models/application_record'
require_relative 'app/models/node'

# Monkey-patch multipart_post gem to remove filename from Content-Disposition
module Parts
  class FilePart
    def build_head(boundary, name, filename, type, content_len, opts = {}, headers = {})
      trans_encoding = opts["Content-Transfer-Encoding"] || "binary"
      content_disposition = opts["Content-Disposition"] || "form-data"

      part = ''
      part << "--#{boundary}\r\n"
      part << "Content-Disposition: #{content_disposition}; name=\"#{name.to_s}\"\r\n"
      part << "Content-Length: #{content_len}\r\n"
      if content_id = opts["Content-ID"]
        part << "Content-ID: #{content_id}\r\n"
      end

      if headers["Content-Type"] != nil
        part <<  "Content-Type: " + headers["Content-Type"] + "\r\n"
      else
        part << "Content-Type: #{type}\r\n"
      end

      part << "Content-Transfer-Encoding: #{trans_encoding}\r\n"
      part << "\r\n"
    end
  end
end





c = Node.establish_connection(adapter: 'sqlite3', database: 'db/development.sqlite3')

# Node.delete_all

WEBDAV_SRC = ENV['WEBDAV_SRC'] || 'http://localhost:8080'
WEBDAV_DST = ENV['WEBDAV_DST'] || 'http://localhost:8080'
WORKSPACE = ENV['WORKSPACE'] || 'default'
USERNAME = ENV['USERNAME'] || 'admin'
PASSWORD = ENV['PASSWORD']
START_PATH = ENV['ROOT']


$DEBUG = false

def perform_requests(ar_node, http_src, http_dest, root_node, depth = 1)
  uri_src = Addressable::URI.parse("#{WEBDAV_SRC}/server/#{WORKSPACE}/jcr%3aroot#{URI.encode(root_node)}.#{depth}.json")
  uri_dest = Addressable::URI.parse("#{WEBDAV_DST}/server/#{WORKSPACE}/jcr%3aroot#{URI.encode(root_node)}.#{depth}.json")

  print "** #{root_node} "

  request_src = Net::HTTP::Get.new(uri_src.request_uri)
  response_src = http_src.request(request_src)
  raise "Failed SRC request: #{root_node}, #{response_src.code}" unless response_src.code == '200'
  data = JSON.parse(response_src.body)

  request_dest = Net::HTTP::Get.new(uri_dest.request_uri)
  response_dest = http_dest.request(request_dest)
  if response_dest.code == '404'
    puts "[creating]".green
    handle_creation(ar_node, data, http_dest, root_node)

  else
    puts "[updating]".yellow
    data_dest = JSON.parse(response_dest.body)
    keys_in_src = data.keys.sort - data_dest.keys.sort
    keys_in_dest = data_dest.keys.sort - data.keys.sort
    keys_in_common = data.keys.sort - keys_in_src
    if keys_in_src.any? or keys_in_dest.any?
      puts "    [diff-keys: src=#{keys_in_src}, dest=#{keys_in_dest}"
      handle_add_remove_keys(data, data_dest, http_dest, root_node, keys_in_src, keys_in_dest)
    end
    changed = handle_merge(data, data_dest, http_dest, root_node, keys_in_common) if keys_in_common
    puts '    [identical]' unless changed or keys_in_src.any? or keys_in_dest.any?

    # Now add any child nodes to be done
    add_all_child_nodes(ar_node, data, root_node)

    puts "\n\n"
  end

  ar_node.update_attributes!(status: :complete, last_synced_at: DateTime.current)
end

def handle_creation(ar_node, data, http_dest, root_node)
  uri_dest = URI("#{WEBDAV_DST}/server/#{WORKSPACE}/jcr%3aroot/")

  if root_node[/[\[\]]/]
    puts "    !!! Skipping invalid duplicate node! #{root_node}"
    return
  end

  # Don't do special JCR properties, or hashes (child nodes?)
  filtered_data = data.reject { |k, _v| k.start_with?(':') }.reject { |_k, v| v.is_a? Hash }
  # ap filtered_data

  request = Net::HTTP::Post::Multipart.new(uri_dest,
                                           ':diff' => UploadIO.new(StringIO.new("+#{root_node} : #{filtered_data.to_json}"), 'text/plain'))
  request.basic_auth USERNAME, PASSWORD

  response_dest = http_dest.request(request)
  raise "Failed POST to DEST: #{root_node}, #{response_dest.code}" unless response_dest.code == '200'
  response_body = JSON.parse(response_dest.body) unless response_dest.body.empty?
  puts "Success! #{response_body}" if $DEBUG

  # Now add any child nodes to be done
  add_all_child_nodes(ar_node, data, root_node)
end

def add_all_child_nodes(ar_node, data, root_node)
  data.each do |k, v|
    case v
      when Hash
        handle_hash_or_child_nodes(ar_node, root_node, k, v)
    end
  end
end

# def handle_creation2(data, _http_dest, root_node)
#   puts "  Create node... to be done"
#   data.each do |k, v|
#     case v
#       when TrueClass, FalseClass  then handle_bool(k, v)
#       when String                 then handle_string(k, v)
#       when Fixnum                 then handle_long(k, v)
#       when Float                  then handle_float(k, v)
#       when Array                  then handle_array(data, k, v)
#       when Hash                   then handle_hash_or_child_nodes(k, v)
#       else
#         raise "  #{k} - unknown\n===\n #{v}\n===\n"
#     end
#   end
#
#   node = Node.create!(path: root_node, status: :incomplete, parent: nil, last_synced_at: DateTime.current)
# end

def handle_add_remove_keys(data, data_dest, http_dest, root_node, keys_in_src, keys_in_dest)

  # Add keys in src, but set merging to false
  handle_merge(data, data_dest, http_dest, root_node, keys_in_src, false)
  handle_remove_properties(http_dest, root_node, keys_in_dest)
end

def handle_merge(data, data_dest, http_dest, root_node, keys_in_common, is_merging = true)
  updates = {}
  differences = false
  keys_in_common.each do |k|
    next if data[k].is_a? Hash
    next unless !is_merging or data[k] != data_dest[k]
    differences = true
    puts "    mismatch in #{k}: #{data[k]}; and: #{data_dest[k]}" if is_merging
    updates[k] = data[k]
  end

  update_text = []
  updates.each do |k, v|
    next if k == 'jcr:uuid'
    if k.start_with?(':')
      puts "    ... ignoring #{k}"
      next
    end

    case v
      when String then update_text.push "^#{root_node}/#{k} : \"#{v.gsub('"', '\"')}\""
      when Fixnum then update_text.push "^#{root_node}/#{k} : #{v}"
      when Float then update_text.push "^#{root_node}/#{k} : #{v}"
      when TrueClass, FalseClass then update_text.push "^#{root_node}/#{k} : #{v}"
      when Array then update_text.push "^#{root_node}/#{k} : #{v}"
      when Hash
      else
        raise "Not handled: #{v.class}"
    end
  end
  return false unless update_text.any?

  uri_dest = URI("#{WEBDAV_DST}/server/#{WORKSPACE}/jcr%3aroot/")
  request = Net::HTTP::Post::Multipart.new(uri_dest,
                                           ':diff' => UploadIO.new(StringIO.new(update_text.join("\r\n")), 'text/plain'))
  request.basic_auth USERNAME, PASSWORD

  response_dest = http_dest.request(request)
  raise "Failed POST to DEST: #{root_node}, #{response_dest.code}" unless response_dest.code == '200'
  response_body = JSON.parse(response_dest.body) unless response_dest.body.empty?
  puts "Successfully merged! #{response_body}" if $DEBUG

  differences
end

def handle_remove_properties(http_dest, root_node, keys_in_dest)
  update_text = []
  keys_in_dest.each do |k|
    next if k == 'jcr:uuid'
    if k.start_with?(':')
      puts "    ... ignoring #{k} when removing properties"
      next
    end

    case v
      when String then update_text.push "-#{root_node}/#{k}"
      when Fixnum then update_text.push "-#{root_node}/#{k}"
      when Float then update_text.push "-#{root_node}/#{k}"
      when TrueClass, FalseClass then update_text.push "-#{root_node}/#{k}"
      when Array then update_text.push "-#{root_node}/#{k}"
      when Hash
      else
        raise "Not handled: #{v.class}"
    end
  end
  return false unless update_text.any?

  uri_dest = URI("#{WEBDAV_DST}/server/#{WORKSPACE}/jcr%3aroot/")
  request = Net::HTTP::Post::Multipart.new(uri_dest,
                                           ':diff' => UploadIO.new(StringIO.new(update_text.join("\r\n")), 'text/plain'))
  request.basic_auth USERNAME, PASSWORD

  response_dest = http_dest.request(request)
  raise "Failed POST to DEST: #{root_node}, #{response_dest.code}" unless response_dest.code == '200'
  response_body = JSON.parse(response_dest.body) unless response_dest.body.empty?
  puts "Successfully removed! #{response_body}" if $DEBUG

end

# def handle_bool(k, v)
#   puts "  #{k}: #{v} (Bool)" if $DEBUG
# end
#
# def handle_string(k, v)
#   puts "  #{k}: #{v} (String)" if $DEBUG
# end
#
# def handle_long(k, v)
#   puts "  #{k}: #{v} (Long)" if $DEBUG
# end
#
# def handle_float(k, v)
#   puts "  #{k}: #{v} (Float)" if $DEBUG
# end
#
# def handle_array(data, property, ary)
#   unless ary.any?
#     puts "  #{property}: Array" if $DEBUG
#     puts "     (empty set of '#{data[":#{property}"]}')" if $DEBUG
#     return
#   end
#
#   # return handle_child_nodes(property, ary) if ary[0].is_a? Hash and ary[0]['jcr:uuid'].present?
#   puts "  #{property}: Array (multi-val property)" if $DEBUG
#   puts "    #{ary}" if $DEBUG
# end
#

def handle_hash_or_child_nodes(ar_node, root_node, property, hash)
  return handle_child_nodes(ar_node, root_node, property, hash) if hash['jcr:primaryType'].present?
  raise "  #{property}: Hash (multi-val property) data: #{hash}"
end

def handle_child_nodes(ar_node, root_node, property, hsh)

  node = Node.where(path: "#{root_node}/#{property}").first_or_create!(status: :incomplete, parent: ar_node, last_synced_at: nil)

  puts "  #{property}: Hash (child nodes)" if $DEBUG
  hsh.each do |k, v|
    next unless v.is_a? Hash
    Node.where(path: "#{root_node}/#{property}/#{k}").first_or_create!(status: :incomplete, parent: node, last_synced_at: nil)
    puts "    - #{k} with UUID #{v['jcr:uuid']}" if $DEBUG
  end
end

def execute!
  uri1 = URI("#{WEBDAV_SRC}/server/")
  uri2 = URI("#{WEBDAV_DST}/server/")

  Net::HTTP.start(uri1.host, uri1.port) do |http_src|
    Net::HTTP.start(uri2.host, uri2.port) do |http_dst|

      root_node = START_PATH
      # start_time = DateTime.current

      # Handle the root node
      ar_node = Node.where(path: root_node).first_or_create!(status: :incomplete, parent: nil, last_synced_at: nil)
      perform_requests(ar_node, http_src, http_dst, root_node, 1)

      # Now search for any nodes to be synced
      Node.where(status: :incomplete).order(:created_at, :id).find_in_batches(batch_size: 100) do |batch|
        batch.each do |node|
          perform_requests(node, http_src, http_dst, node.path, 1)
        end
      end
    end
  end
end

execute!