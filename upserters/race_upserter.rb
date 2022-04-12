require 'mysql'
require 'logger'
require_relative './upserter.rb'

class RaceUpserter < Upserter
  FILENAME_PREFIX = 'Bac/Bac'.freeze

  def initialize(filedate)
    @filepath = BASE_FILE_PATH + FILENAME_PREFIX + filedate + FILE_EXTENTION
    @current_date = Time.now
    @logger = Logger.new(STDOUT)
  end

  def upsert
    return unless File.exist?(@filepath)

    parse.each do |line|
      unless course_id(line)
        course_insert(line)
      end

      unless held_id(line)
        held_insert(line)
      end

      statement = client.prepare(race_query)
      if statement.execute(course_id(line), held_id(line), line[:race_round]).size == 0
        race_insert(line)
      end
    end

    postprocess

    @logger.info("File #{@filepath} reflected in the database.")
  end

  private

  def course_id(line)
    course_statement = client.prepare(course_query)
    course = course_statement.execute(
      line[:racecourse_id],
      line[:coursetype_id],
      line[:course_length]
    ).first
    course.first if course
  end

  def held_id(line)
    held_statement = client.prepare(held_query)
    held = held_statement.execute(
      line[:racecourse_id],
      line[:held_year],
      line[:number_of_times],
      line[:number_of_days],
    ).first
    held.first if held
  end

  def course_insert(line)
    statement = client.prepare(course_insert_statement) 
    statement.execute(
      line[:racecourse_id],
      line[:coursetype_id],
      line[:course_length],
      @current_date,
      @current_date
    )
  end

  def held_insert(line)
    statement = client.prepare(held_insert_statement) 
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

  def race_insert(line)
    statement = client.prepare(race_insert_statement) 
    statement.execute(
      line[:race_name],
      course_id(line),
      held_id(line),
      line[:race_round],
      @current_date,
      @current_date
    )
  end

  def client
    Mysql.connect(
      "mysql://#{ENV['MYSQL_USER']}:#{ENV['MYSQL_PASSWORD']}@#{ENV['MYSQL_HOST']}:#{ENV['MYSQL_PORT']}/#{ENV['MYSQL_DATABASE']}?charset=utf8mb4"
    )
  end

  def course_query
    'SELECT id FROM courses WHERE racecourse_id = ? AND coursetype_id = ? AND course_length = ?'
  end

  def held_query
    'SELECT id FROM helds WHERE racecourse_id = ? AND held_year = ? AND number_of_times = ? AND number_of_days = ?'
  end

  def race_query
    'SELECT id FROM races WHERE course_id = ? AND held_id = ? AND race_round = ?'
  end

  def race_insert_statement
    'INSERT INTO races (name, course_id, held_id, race_round, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)'
  end

  def held_insert_statement
    'INSERT INTO helds (racecourse_id, held_year, held_month, held_day, number_of_times, number_of_days, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
  end

  def course_insert_statement
    'INSERT INTO courses (racecourse_id, coursetype_id, course_length, created_at, updated_at) VALUES (?, ?, ?, ?, ?)'
  end

  def parse
    CSV.read(@filepath, headers: true).map do |line|
      {
        racecourse_id: line[0].to_i,
        held_year: line[1].to_i,
        number_of_times: line[2].to_i,
        number_of_days: line[3].to_i(16),
        race_round: line[4].to_i,
        held_month: line[5][4..5].to_i,
        held_day: line[5][6..7].to_i,
        race_name: line[22].trim,
        coursetype_id: line[8].to_i,
        course_length: line[7].to_i
      }
    end
  end

  def postprocess
    File.delete(@filepath)
  end
end
