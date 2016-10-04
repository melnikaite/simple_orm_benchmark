require 'sequel'
require 'json'
require 'logger'
DB = Sequel.connect(ORM_CONFIG)
if ORM_CONFIG['debug']
  DB.loggers << Logger.new($stdout)
  DB.sql_log_level = :debug
end

DB.drop_table(:people) rescue nil
DB.drop_table(:parties) rescue nil
DB.extension :pg_json if ORM_CONFIG['adapter']=='postgres'

DB.create_table(:parties, :engine=>:InnoDB) do
  primary_key :id
  String :theme
  if ORM_CONFIG['adapter']=='postgres'
    Jsonb :stuff
  else
    String :stuff, :text=>true
  end
end

DB.create_table(:people, :engine=>:InnoDB) do
  primary_key :id
  foreign_key :party_id, :parties, :key=>:id
  foreign_key :other_party_id, :parties, :key=>:id
  String :name
  String :address
end

class Party < Sequel::Model
  plugin :prepared_statements
  plugin :serialization, :json, :stuff unless ORM_CONFIG['adapter']=='postgres'
  one_to_many :people 
  one_to_many :other_people, :class=>:Person, :key=>:other_party_id
end

class Person < Sequel::Model  
  plugin :prepared_statements
  many_to_one :party
  many_to_one :other_party, :class=>:Party
end

class Bench
  def delete_all
    DB << "DELETE FROM people"
    DB << "DELETE FROM parties"
  end

  def all_parties
    Party.all
  end

  def get_party(id)
    Party[id]
  end

  def get_party_hash(id)
    Party.find(:id=>id)
  end

  def get_party_hash_deep
    if ORM_CONFIG['adapter']=='postgres'
      Party.find("stuff->>'pumpkin' = ?", '1')
    else
      Party.find("json_extract(stuff, '$.pumpkin') = ?", 1)
    end
  end

  def update_party_hash_deep(id)
    if ORM_CONFIG['adapter']=='postgres'
      Party.where(id: id).update("stuff = jsonb_set(stuff::jsonb, '{pumpkin}', '2')")
    else
      Party.where(id: id).update("stuff = JSON_SET(stuff, '$.pumpkin', '2')")
    end
  end

  def update_party_hash_full(id)
    if ORM_CONFIG['adapter']=='postgres'
      Party.where(id: id).update(:stuff=>Sequel.pg_json({pumpkin: 2, candy: 1}))
    else
      Party.where(id: id).update(:stuff=>{pumpkin: 2, candy: 1}.to_json)
    end
  end

  def eager_graph_party_both_people
    Party.filter('people.id = people.id AND other_people.id=other_people.id').eager_graph(:people, :other_people).all{|party| party.people.each{|p| p.id}; party.other_people.each{|p| p.id}}
  end

  def eager_graph_party_people
    Party.filter('people.id = people.id').eager_graph(:people).all{|party| party.people.each{|p| p.id}}
  end

  def eager_load_party_both_people
    Party.eager(:people, :other_people).all{|party| party.people.size; party.other_people.each{|p| p.id}}
  end

  def eager_load_party_people
    Party.eager(:people).all{|party| party.people.each{|p| p.id}}
  end
  
  def first_party
    Party.first
  end

  def insert_party(times)
    times.times{Party.create(:theme=>'Halloween', :stuff=>{pumpkin: 1, candy: 1})}
  end

  def insert_party_people(times, people_per_party)
    times.times do
      p = Party.create(:theme=>'Halloween')
      people_per_party.times{Person.create(:name=>"Party_#{p.id}", :party_id=>p.id)}
    end
  end

  def insert_party_both_people(times, people_per_party)
    times.times do
      p = Party.create(:theme=>'Halloween')
      people_per_party.times do
        Person.create(:name=>"Party_#{p.id}", :party_id=>p.id)
        Person.create(:name=>"Party_#{p.id}", :other_party_id=>p.id)
      end
    end
  end

  def transaction(&block)
    DB.transaction(&block)
  end
  
  def with_connection
    yield
  end

  def self.drop_tables
    DB.drop_table(:people, :parties)
  end

  def self.support_json?
    begin
      case ORM_CONFIG['adapter']
        when 'sqlite'
          DB['select json("{}")'].to_a[0][:'json("{}")'] == '{}'
        when 'mysql2'
          DB["select JSON_OBJECT()"].to_a[0][:'JSON_OBJECT()'] == '{}'
        when 'postgres'
          DB["select json_object('{}')"].to_a[0][:json_object] == '{}'
        else
          false
      end
    rescue
      false
    end
  end
end
