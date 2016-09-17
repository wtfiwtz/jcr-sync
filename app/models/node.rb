class Node < ApplicationRecord
  enum status: { incomplete: 1, complete: 2 }

  belongs_to :parent, class_name: 'Node', inverse_of: :children
  has_many :children, class_name: 'Node', foreign_key: :parent_id
end
