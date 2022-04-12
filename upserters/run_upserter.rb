require 'mysql'
require 'logger'
require 'csv'
require_relative './upserter.rb'

class RunUpserter < Upserter
  FILENAME_PREFIX = 'Kta/Kta'.freeze

  def initialize(filedate)
    @filepath = BASE_FILE_PATH + FILENAME_PREFIX + filedate + FILE_EXTENTION
    @current_date = Time.now
    @logger = Logger.new(STDOUT)
  end

  def upsert
    return unless File.exist?(@filepath)

    parse.each do |line|
      statement = client.prepare(query)
      if statement.execute(line[:horse_id], race_id(line)).size == 0
        insert(line)
      else
        update(line)
      end
    end

    postprocess

    @logger.info("File #{@filepath} reflected in the database.")
  end

  private

  def race_id(line)
    race_statement = client.prepare(race_query)
    race = race_statement.execute(
      line[:race_round],
      line[:racecourse_id],
      line[:held_year],
      line[:number_of_times],
      line[:number_of_days],
    ).first
    race.first if race
  end

  def insert(line)
    statement = client.prepare(insert_statement) 
    statement.execute(
      line[:horse_id],
      race_id(line),
      line[:burden_weight],
      line[:jockey_id],
      @current_date,
      @current_date
    )
  end

  def update(line)
    statement = client.prepare(query)
    id = statement.execute(line[:horse_id], race_id(line)).first.first

    statement = client.prepare(update_statement) 
    statement.execute(
      line[:burden_weight],
      line[:jockey_id], 
      @current_date,
      id
    )
  end

  def client
    @connection ||= Mysql.connect(
      "mysql://#{ENV['MYSQL_USER']}:#{ENV['MYSQL_PASSWORD']}@#{ENV['MYSQL_HOST']}:#{ENV['MYSQL_PORT']}/#{ENV['MYSQL_DATABASE']}?charset=utf8mb4"
    )
  end

  def query
    'SELECT id FROM runs WHERE horse_id = ? AND race_id = ?'
  end

  def race_query
    'SELECT r.* FROM races r INNER JOIN helds h ON r.held_id = h.id WHERE r.race_round = ? AND h.racecourse_id = ? AND h.held_year = ? AND h.number_of_times = ? AND h.number_of_days = ?'
  end

  def insert_statement
    'INSERT INTO runs (horse_id, race_id, burden_weight, jockey_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)'
  end

  def update_statement
    'UPDATE runs SET burden_weight = ?, jockey_id = ?, updated_at = ? WHERE id = ?'
  end

  def parse
    CSV.read(@filepath, headers: true).map do |line|
      {
        horse_id: line[6].to_i,
        racecourse_id: line[0].to_i,
        held_year: line[1].to_i,
        number_of_times: line[2].to_i,
        number_of_days: line[3].to_i(16),
        race_round: line[4].to_i,
        burden_weight: line[12].to_f / 10,
        jockey_id: line[38].trim.to_i,
      }
    end
  end

  def postprocess
    @connection.close
    File.delete(@filepath)
  end
end
