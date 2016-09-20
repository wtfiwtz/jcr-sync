# Mirror the contents of a JCR from one repository to another
#
# To use this script:
#
#   WEBDAV_SRC=http://localhost:8080 WEBDAV_DST=http://localhost:8080 PASSWORD=pw ROOT=/jcrpath ruby test.rb

require 'active_record'
require 'net/http'
require 'net/http/post/multipart'
require 'awesome_print'
require 'json'
require 'addressable/uri'

require_relative 'app/models/application_record'
require_relative 'app/models/node'

# Monkey-patch multipart_post gem to add/remove filename from Content-Disposition
module Parts
  class FilePart
    def build_head(boundary, name, filename, type, content_len, opts = {}, headers = {})
      trans_encoding = opts["Content-Transfer-Encoding"] || "binary"
      content_disposition = opts["Content-Disposition"] || "form-data"

      # If we are submitting a binary value, then we should add the filename so we get streaming form fields
      # in the JCR servlet... the filename can simply be the name
      with_filename = (opts['Content-Type'] || '').include?('jcr-value/binary')

      part = ''
      part << "--#{boundary}\r\n"
      if with_filename
        part << "Content-Disposition: #{content_disposition}; name=\"#{name.to_s}\"; filename=\"#{name.to_s}\"\r\n"
      else
        part << "Content-Disposition: #{content_disposition}; name=\"#{name.to_s}\"\r\n"
      end
      part << "Content-Length: #{content_len}\r\n"
      if content_id = opts["Content-ID"]
        part << "Content-ID: #{content_id}\r\n"
      end

      # NOTE: Also, switch 'headers' for 'opt'
      if opts["Content-Type"] != nil
        part <<  "Content-Type: " + opts["Content-Type"] + "\r\n"
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
DEPTH = 2
RECURSIVE = true

DATE_MIME_TYPE = 'jcr-value/date'
BINARY_MIME_TYPE = 'jcr-value/binary'

$DEBUG = false
$DEBUG_DISPLAY = false

def perform_requests(ar_node, http_src, http_dest, root_node)
  uri_src = Addressable::URI.parse("#{WEBDAV_SRC}/server/#{WORKSPACE}/jcr%3aroot#{URI.encode(root_node)}.#{DEPTH}.json")
  uri_dest = Addressable::URI.parse("#{WEBDAV_DST}/server/#{WORKSPACE}/jcr%3aroot#{URI.encode(root_node)}.#{DEPTH}.json")

  print "** #{root_node} "

  request_src = Net::HTTP::Get.new(uri_src.request_uri)
  response_src = http_src.request(request_src)
  raise "Failed SRC request: #{root_node}, #{response_src.code}" unless response_src.code == '200'
  data = JSON.parse(response_src.body)

  request_dest = Net::HTTP::Get.new(uri_dest.request_uri)
  response_dest = http_dest.request(request_dest)
  not_found = response_dest.code == '404'

  if not_found
    puts "[creating]".green
    display(ar_node, http_src, http_dest, data, root_node) if $DEBUG_DISPLAY
    handle_creation(ar_node, data, http_src, http_dest, root_node)

  else
    puts "[updating]".yellow
    display(ar_node, http_src, http_dest, data, root_node) if $DEBUG_DISPLAY
    data_dest = JSON.parse(response_dest.body)
    keys_in_src = data.keys.sort - data_dest.keys.sort
    keys_in_dest = data_dest.keys.sort - data.keys.sort
    keys_in_common = data.keys.sort - keys_in_src
    if keys_in_src.any? or keys_in_dest.any?
      puts "    [diff-keys: src=#{keys_in_src}, dest=#{keys_in_dest}"
      handle_add_remove_keys(data, data_dest, http_src, http_dest, root_node, keys_in_src, keys_in_dest)
    end
    changed = handle_merge(data, data_dest, http_src, http_dest, root_node, keys_in_common) if keys_in_common
    puts '    [identical]'.yellow unless changed or keys_in_src.any? or keys_in_dest.any?

    # Now add any child nodes to be done
    add_all_child_nodes(ar_node, http_src, http_dest, data, root_node)

    puts "\n\n"
  end

  ar_node.update_attributes!(status: :complete, last_synced_at: DateTime.current)
end

def handle_creation(ar_node, data, http_src, http_dest, root_node, subnode = false)
  uri_dest = URI("#{WEBDAV_DST}/server/#{WORKSPACE}/jcr%3aroot/")

  if root_node[/[\[\]]/]
    puts "    !!! Skipping invalid duplicate node! #{root_node}".red
    return
  end

  # Don't do special JCR properties, or hashes (child nodes?)
  filtered_data = data.reject { |k, _v| k.start_with?(':') }.reject { |_k, v| v.is_a? Hash }

  # Handle dates and binary field separately
  date_keys = data.select { |k, v| k.start_with?(':') and v == 'Date' }.keys.collect { |k| k.gsub(/^:/, '')}
  binary_keys = data.select { |k, v| k.start_with?(':') and !k.start_with?('::') and v.is_a? Fixnum }.keys.collect { |k| k.gsub(/^:/, '')}
  filtered_data.delete_if { |k, _v| date_keys.include?(k) or binary_keys.include?(k) }

  # if is this a subnode, then only create if we don't already have a record
  prev_record = Node.where(path: root_node).first if subnode
  if not subnode or not prev_record
    request = Net::HTTP::Post::Multipart.new(uri_dest, ':diff' =>
        UploadIO.new(StringIO.new("+#{root_node} : #{filtered_data.to_json}"), 'text/plain'))
    request.basic_auth USERNAME, PASSWORD
    response_dest = http_dest.request(request)

    # If we are adding a subnode, they can fail if they already exist... just ignore and continue and the update will
    # pick up the changes later
    if subnode and response_dest.code == '403'
      puts "    #{root_node} already exists".blue
      return
    end

    raise "Failed POST to DEST: #{root_node}, #{response_dest.code}".red unless response_dest.code == '200'
    response_body = JSON.parse(response_dest.body) unless response_dest.body.empty?
    puts "Success! #{response_body}" if $DEBUG
    puts "    #{root_node}".green if subnode
  else
    puts "    #{root_node} exists".blue if subnode
  end

  # Now add any date and binary keys
  handle_date_and_binary_properties(data, http_src, http_dest, root_node, date_keys, binary_keys) unless prev_record

  # Now add any child nodes to be done (only at the top level)
  add_all_child_nodes(ar_node, http_src, http_dest, data, root_node) unless subnode
end

def handle_date_and_binary_properties(data, http_src, http_dest, root_node, date_keys, binary_keys)

  uri_dest = URI("#{WEBDAV_DST}/server/#{WORKSPACE}/jcr%3aroot/")

  date_keys.each do |k|
    # TODO: More efficient ways to set dates? multipart boundaries?
    request = Net::HTTP::Post::Multipart.new(uri_dest, "#{root_node}/#{k}" =>
        UploadIO.new(StringIO.new(data[k]), DATE_MIME_TYPE))
    request.basic_auth USERNAME, PASSWORD
    response_dest = http_dest.request(request)
    raise "Failed POST (dates) to DEST: #{root_node}, #{response_dest.code}".red unless response_dest.code == '200'
    response_body = JSON.parse(response_dest.body) unless response_dest.body.empty?
    puts "         date: #{k}".blue
    puts "Success! #{response_body}" if $DEBUG
  end

  binary_keys.each do |k|

    binary_uri = Addressable::URI.parse("#{WEBDAV_SRC}/server/#{WORKSPACE}/jcr%3aroot#{URI.encode(root_node)}/#{k}")
    binary_req = Net::HTTP::Get.new(binary_uri)
    binary_resp = http_src.request(binary_req)
    raise "Failed GET (binary) from SRC: #{root_node}/#{k}, #{binary_resp.code}".red unless binary_resp.code == '200'

    request = Net::HTTP::Post::Multipart.new(uri_dest,
        { "#{root_node}/#{k}" => UploadIO.new(StringIO.new(binary_resp.body), BINARY_MIME_TYPE) },
        { parts: { "#{root_node}/#{k}" => { 'Content-Type' => "#{BINARY_MIME_TYPE}" } } })
    request.basic_auth USERNAME, PASSWORD
    response_dest = http_dest.request(request)
    raise "Failed POST (binary) to DEST: #{root_node}, #{response_dest.code}".red unless response_dest.code == '200'
    response_body = JSON.parse(response_dest.body) unless response_dest.body.empty?
    puts "         binary: #{k} (len=#{binary_resp.body.size})".blue
    puts "Success! #{response_body}" if $DEBUG
  end
end

def add_all_child_nodes(ar_node, http_src, http_dest, data, root_node)
  data.each do |k, v|
    case v
      when Hash
        handle_hash_or_child_nodes(ar_node, http_src, http_dest, root_node, k, v)
    end
  end
end

def display(ar_node, http_src, http_dest, data, root_node)
  data.each do |k, v|
    case v
      when TrueClass, FalseClass  then display_bool(k, v)
      when String                 then display_string(k, v)
      when Fixnum                 then display_long(k, v)
      when Float                  then display_float(k, v)
      when Array                  then display_array(data, k, v)
      when Hash                   then handle_hash_or_child_nodes(ar_node, http_src, http_dest, root_node, k, v, false)
      else
        raise "  #{k} - unknown\n===\n #{v}\n===\n"
    end
  end
end

def handle_add_remove_keys(data, data_dest, http_src, http_dest, root_node, keys_in_src, keys_in_dest)

  if root_node[/[\[\]]/]
    puts "    !!! Skipping invalid duplicate node! #{root_node}".red
    return
  end

  # Add keys in src, but set merging to false
  handle_merge(data, data_dest, http_src, http_dest, root_node, keys_in_src, false)
  handle_remove_properties(http_dest, data_dest, root_node, keys_in_dest)
end

def handle_merge(data, data_dest, http_src, http_dest, root_node, keys_in_common, is_merging = true)
  updates = {}
  differences = false
  keys_in_common.each do |k|
    next if data[k].is_a? Hash
    next unless !is_merging or data[k] != data_dest[k]
    differences = true
    puts "    mismatch in #{k}: #{data[k]}; and: #{data_dest[k]}" if is_merging
    updates[k] = data[k]
  end

  # Handle dates and binary field separately (if they require merging)
  date_keys = updates.select { |k, v| k.start_with?(':') and v == 'Date' }.keys.collect { |k| k.gsub(/^:/, '')}
  binary_keys = updates.select { |k, v| k.start_with?(':') and !k.start_with?('::') and v.is_a? Fixnum }.keys.collect { |k| k.gsub(/^:/, '')}
  filtered_data = updates.reject { |k, _v| date_keys.include?(k) or binary_keys.include?(k) }

  update_text = []
  filtered_data.each do |k, v|
    next if k == 'jcr:uuid'
    next if k.start_with?(':')

    case v
      when String then update_text.push "^#{root_node}/#{k} : \"#{string_subst(v)}\""
      when Fixnum then update_text.push "^#{root_node}/#{k} : #{v}"
      when Float then update_text.push "^#{root_node}/#{k} : #{v}"
      when TrueClass, FalseClass then update_text.push "^#{root_node}/#{k} : #{v}"
      when Array then update_text.push "^#{root_node}/#{k} : #{v}"
      when Hash
      else
        raise "Not handled: #{v.class}".red
    end
  end

  if update_text.any?
    uri_dest = URI("#{WEBDAV_DST}/server/#{WORKSPACE}/jcr%3aroot/")
    request = Net::HTTP::Post::Multipart.new(uri_dest,
                                             ':diff' => UploadIO.new(StringIO.new(update_text.join("\r\n")), 'text/plain'))
    request.basic_auth USERNAME, PASSWORD

    response_dest = http_dest.request(request)
    raise "Failed POST to DEST: #{root_node}, #{response_dest.code}".red unless response_dest.code == '200'
    response_body = JSON.parse(response_dest.body) unless response_dest.body.empty?
    puts "Successfully merged! #{response_body}" if $DEBUG
  end

  # Now handle date and binary properties separately
  handle_date_and_binary_properties(data, http_src, http_dest, root_node, date_keys, binary_keys) if date_keys.any? or binary_keys.any?

  differences
end

def string_subst(v)
  return '' unless v
  splits = v.split("\n")
  merged = splits.collect do |line|
    {
        '"' => '\"',
        "\b" => '\b',
        "\t" => '\t',
        "\f" => '\f',
        "\r" => '\r'

    }.each do |k, v|
      line.gsub!(k, v)
    end

    line
  end.join('\n') # Use a literal \n, not a real newline

  merged
end

def handle_remove_properties(http_dest, data_dest, root_node, keys_in_dest)
  update_text = []
  keys_in_dest.each do |k|
    next if k == 'jcr:uuid'
    next if k.start_with?(':')

    case data_dest[k]
      when String then update_text.push "-#{root_node}/#{k} : "
      when Fixnum then update_text.push "-#{root_node}/#{k} : "
      when Float then update_text.push "-#{root_node}/#{k} : "
      when TrueClass, FalseClass then update_text.push "-#{root_node}/#{k} : "
      when Array then update_text.push "-#{root_node}/#{k} : "
      when Hash
      else
        raise "Not handled: #{v.class}".red
    end
  end
  return false unless update_text.any?

  uri_dest = URI("#{WEBDAV_DST}/server/#{WORKSPACE}/jcr%3aroot/")
  request = Net::HTTP::Post::Multipart.new(uri_dest,
                                           ':diff' => UploadIO.new(StringIO.new(update_text.join("\r\n")), 'text/plain'))
  request.basic_auth USERNAME, PASSWORD

  response_dest = http_dest.request(request)
  raise "Failed POST to DEST: #{root_node}, #{response_dest.code}".red unless response_dest.code == '200'
  response_body = JSON.parse(response_dest.body) unless response_dest.body.empty?
  puts "Successfully removed! #{response_body}" if $DEBUG

end

def display_bool(k, v)
  puts "  #{k}: #{v} (Bool)" if $DEBUG
end

def display_string(k, v)
  puts "  #{k}: #{v} (String)" if $DEBUG
end

def display_long(k, v)
  puts "  #{k}: #{v} (Long)" if $DEBUG
end

def display_float(k, v)
  puts "  #{k}: #{v} (Float)" if $DEBUG
end

def display_array(data, property, ary)
  unless ary.any?
    puts "  #{property}: Array" if $DEBUG
    puts "     (empty set of '#{data[":#{property}"]}')" if $DEBUG
    return
  end

  puts "  #{property}: Array (multi-val property)" if $DEBUG
  puts "    #{ary}" if $DEBUG
end


def handle_hash_or_child_nodes(ar_node, http_src, http_dest, root_node, property, hash, create = true)
  return handle_child_nodes(ar_node, http_src, http_dest, root_node, property, hash, create) if hash['jcr:primaryType'].present?
  return if hash == {} or not create # happens with sub-nodes?
  raise "  #{property}: Hash (multi-val property) data: #{hash}".red
end

def handle_child_nodes(ar_node, http_src, http_dest, root_node, property, hsh, create = true)

  node = Node.where(path: "#{root_node}/#{property}").first_or_create!(status: :incomplete, parent: ar_node, last_synced_at: nil) if create

  puts "  #{property}: Hash (child nodes)" if $DEBUG
  hsh.each do |k, v|
    puts "    - #{k}: #{v}" if $DEBUG_DISPLAY
    next unless v.is_a? Hash
    Node.where(path: "#{root_node}/#{property}/#{k}").first_or_create!(status: :incomplete, parent: node, last_synced_at: nil) if create
    puts "    - #{k} with UUID #{v['jcr:uuid']}" if $DEBUG and not $DEBUG_DISPLAY
  end

  if create and DEPTH >= 2
    handle_creation(ar_node, hsh, http_src, http_dest, "#{root_node}/#{property}", true)
    # perform_requests(ar_node, http_src, http_dest, "#{root_node}/#{property}")
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
      perform_requests(ar_node, http_src, http_dst, root_node)

      # Now search for any nodes to be synced
      if RECURSIVE
        Node.where(status: :incomplete).order(:created_at, :id).find_in_batches(batch_size: 100) do |batch|
          batch.each do |node|
            perform_requests(node, http_src, http_dst, node.path)
          end
        end
      end
    end
  end
end

execute!