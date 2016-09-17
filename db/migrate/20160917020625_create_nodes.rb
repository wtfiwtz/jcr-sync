class CreateNodes < ActiveRecord::Migration[5.0]
  def change
    create_table :nodes do |t|
      t.text :path
      t.references :parent, foreign_key: true
      t.integer :status
      t.datetime :last_synced_at

      t.timestamps
    end
    add_index :nodes, :path
    add_index :nodes, :status
  end
end
