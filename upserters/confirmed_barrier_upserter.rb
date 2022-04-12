require 'mysql'
require 'logger'
require 'csv'
require_relative './upserter.rb'

class ConfirmedBarrierUpserter < Upserter
  FILENAME_PREFIX = 'Kyi/Kyi'.freeze

  def initialize(filedate)
    @filepath = BASE_FILE_PATH + FILENAME_PREFIX + filedate + FILE_EXTENTION
    @current_date = Time.now
    @logger = Logger.new(STDOUT)
  end

  def upsert
    return unless File.exist?(@filepath)

    parse.each do |line|
      statement = client.prepare(query)
      if statement.execute(run_id(line)).size == 0
        insert(line)
      else
        update(line)
      end
    end

    postprocess

    @logger.info("File #{@filepath} reflected in the database.")
  end

  private

  def run_id(line)
    statement = client.prepare(run_query)
    run = statement.execute(
      line[:horse_id],
      line[:racecourse_id],
      line[:held_year],
      line[:number_of_times],
      line[:number_of_days],
      line[:race_round],
    ).first
    run.first if run
  end

  def insert(line)
    statement = client.prepare(insert_statement) 
    statement.execute(
      run_id(line),
      line[:bracket_number],
      line[:horse_number],
      @current_date,
      @current_date
    )
  end

  def update(line)
    statement = client.prepare(query)
    id = statement.execute(run_id(line)).first.first

    statement = client.prepare(update_statement) 
    statement.execute(
      line[:bracket_number],
      line[:horse_number], 
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
    'SELECT run_id FROM confirmed_barriers WHERE run_id = ?'
  end

  def run_query
    'SELECT ru.id FROM runs ru INNER JOIN races ra ON ru.race_id = ra.id INNER JOIN helds h ON ra.held_id = h.id WHERE ru.horse_id = ? AND h.racecourse_id = ? AND h.held_year = ? AND h.number_of_times = ? AND h.number_of_days = ? AND ra.race_round = ?'
  end

  def insert_statement
    'INSERT INTO confirmed_barriers (run_id, bracket_number, horse_number, created_at, updated_at) VALUES (?, ?, ?, ?, ?)'
  end

  def update_statement
    'UPDATE confirmed_barriers SET bracket_number = ?, horse_number = ?, updated_at = ? WHERE run_id = ?'
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
        bracket_number: line[60].to_i,
        horse_number: line[5].to_i
      }
    end
  end

  def postprocess
    @connection.close
    File.delete(@filepath)
  end
end
