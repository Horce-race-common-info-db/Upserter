require 'mysql'
require 'logger'
require 'csv'
require_relative './upserter.rb'

class JockeyUpserter < Upserter
  FILENAME_PREFIX = 'ks/Ks'.freeze

  def initialize(filedate)
    @filepath = BASE_FILE_PATH + FILENAME_PREFIX + filedate + FILE_EXTENTION
    @current_date = Time.now
    @logger = Logger.new(STDOUT)
  end

  def upsert
    return unless File.exist?(@filepath)

    parse.each do |line|
      statement = client.prepare(query)
      if statement.execute(line[:jockey_code]).size == 0
        insert(line)
      else
        update(line)
      end
    end

    postprocess

    @logger.info("File #{@filepath} reflected in the database.")
  end

  private

  def insert(line)
    statement = client.prepare(insert_statement) 
    statement.execute(line[:jockey_code], line[:jockey_name], @current_date, @current_date)
  end

  def update(line)
    statement = client.prepare(update_statement) 
    statement.execute(line[:jockey_name], @current_date, line[:jockey_code])
  end

  def client
    @connention ||= Mysql.connect(
      "mysql://#{ENV['MYSQL_USER']}:#{ENV['MYSQL_PASSWORD']}@#{ENV['MYSQL_HOST']}:#{ENV['MYSQL_PORT']}/#{ENV['MYSQL_DATABASE']}?charset=utf8mb4"
    )
  end

  def insert_statement
    'INSERT INTO jockeys (id, name, created_at, updated_at) VALUES (?, ?, ?, ?)'
  end

  def update_statement
    'UPDATE jockeys SET name = ?, updated_at = ? WHERE id = ?'
  end

  def query
    'SELECT id FROM jockeys WHERE id = ?'
  end

  def parse
    CSV.read(@filepath, headers: true).map do |line|
      {
        jockey_code: line[0].to_i,
        jockey_name: line[3].trim
      }
    end
  end

  def postprocess
    @connention.close
    File.delete(@filepath)
  end
end
