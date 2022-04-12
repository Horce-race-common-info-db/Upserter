require 'mysql'
require 'logger'
require 'csv'
require_relative './upserter.rb'

class HorseUpserter < Upserter
  FILENAME_PREFIX = 'Ukc/Ukc'.freeze

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
      if statement.execute(line[:horse_code]).size == 0
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
    statement.execute(
      line[:horse_code],
      line[:horse_name],
      line[:sex_code],
      line[:birthday],
      @current_date,
      @current_date
    )
  end

  def update(line)
    statement = client.prepare(update_statement) 
    statement.execute(
      line[:horse_name],
      line[:sex_code], 
      line[:birthday],
      @current_date,
      line[:horse_code]
    )
  end

  def client
    @connection ||= Mysql.connect(
      "mysql://#{ENV['MYSQL_USER']}:#{ENV['MYSQL_PASSWORD']}@#{ENV['MYSQL_HOST']}:#{ENV['MYSQL_PORT']}/#{ENV['MYSQL_DATABASE']}?charset=utf8mb4"
    )
  end

  def insert_statement
    'INSERT INTO horses (id, name, sex_id, birthday, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)'
  end

  def update_statement
    'UPDATE horses SET name = ?, sex_id = ?, birthday = ?, updated_at = ? WHERE id = ?'
  end

  def query
    'SELECT id FROM horses WHERE id = ?'
  end

  def parse
    CSV.read(@filepath, headers: true, encoding: 'UTF-8:UTF-8').map do |line|
      {
        horse_code: line[0].to_i,
        horse_name: line[1].trim,
        sex_code: line[2].to_i,
        birthday: line[8].trim
      }
    end
  end

  def postprocess
    @connection.close
    File.delete(@filepath)
  end
end
