class Stats < ActiveRecord::Migration
  def up
    create_table :stats do |t|
      t.string :word
      t.integer :wiki_freq
      t.integer :ngram_freq
      t.timestamps
    end
  end

  def down
    drop_table :stats
  end
end
