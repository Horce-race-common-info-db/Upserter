require 'mysql'
require 'logger'
require_relative './upserter.rb'

class HeldUpserter < Upserter
  FILENAME_PREFIX = 'Kab/Kab'.freeze

  def initialize(filedate)
    @filepath = BASE_FILE_PATH + FILENAME_PREFIX + filedate + FILE_EXTENTION
    @current_date = Time.now
    @logger = Logger.new(STDOUT)
  end

  def upsert
    unless File.exist?(@filepath)
      @logger.info("File #{@filepath} does not exist.")
      return
    end

    parse.each do |line|
      statement = client.prepare(query)
      if statement.execute(line[:racecourse_id], line[:held_year], line[:number_of_times], line[:number_of_days]).size == 0
        insert(line)
      end
    end

    postprocess

    @logger.info("File #{@filepath} reflected in the database.")
  end

  private

  def insert(line)
    statement = client.prepare(insert_statement) 
    statement.execute(
      line[:racecourse_id],
      line[:held_year],
      line[:held_month],
      line[:held_day],
      line[:number_of_times],
      line[:number_of_days],
      @current_date,
      @current_date
    )
  end

  def client
    @connection ||= Mysql.connect(
      "mysql://#{ENV['MYSQL_USER']}:#{ENV['MYSQL_PASSWORD']}@#{ENV['MYSQL_HOST']}:#{ENV['MYSQL_PORT']}/#{ENV['MYSQL_DATABASE']}?charset=utf8mb4"
    )
  end

  def query
    'SELECT id FROM helds WHERE racecourse_id = ? AND held_year = ? AND number_of_times = ? AND number_of_days = ?'
  end

  def insert_statement
    'INSERT INTO helds (racecourse_id, held_year, held_month, held_day, number_of_times, number_of_days, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
  end

  def parse
    CSV.read(@filepath, headers: true, encoding: 'UTF-8:UTF-8').map do |line|
      {
        racecourse_id: line[0].to_i,
        held_year: line[1].to_i,
        number_of_times: line[2].to_i,
        number_of_days: line[3].to_i(16),
        held_month: line[4][4..5].to_i,
        held_day: line[4][6..7].to_i,
      }
    end
  end

  def postprocess
    @connection.close
    File.delete(@filepath)
  end
end
