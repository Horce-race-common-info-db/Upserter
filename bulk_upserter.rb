require_relative 'upserters/horse_upserter.rb'
require_relative 'upserters/jockey_upserter.rb'
require_relative 'upserters/race_upserter.rb'
require_relative 'upserters/run_upserter.rb'
require_relative 'upserters/confirmed_barrier_upserter.rb'
require_relative 'upserters/result_upserter.rb'

BASE_FILE_PATH = ENV['CONVERT_FILE_OUTPUT_DIRECTORY'].freeze

# 馬基本データ: Ukc
DATA_TYPE = ARGV[0]

case DATA_TYPE
when 'Bac' then
  Dir.glob("#{BASE_FILE_PATH}/Bac/*").each do |filepath|
    filedate = filepath[/#{DATA_TYPE.downcase}(.*?).csv/, 1]
    RaceUpserter.new(filedate).upsert
  end
when 'Ks' then
  Dir.glob("#{BASE_FILE_PATH}/Ks/*").each do |filepath|
    filedate = filepath[/kza(.*?).csv/, 1]
    JockeyUpserter.new(filedate).upsert
  end
when 'Kta' then
  Dir.glob("#{BASE_FILE_PATH}/Kta/*").each do |filepath|
    filedate = filepath[/#{DATA_TYPE.downcase}(.*?).csv/, 1]
    RunUpserter.new(filedate).upsert
  end
when 'Kyi' then
  Dir.glob("#{BASE_FILE_PATH}/Kyi/*").each do |filepath|
    filedate = filepath[/#{DATA_TYPE.downcase}(.*?).csv/, 1]
    ConfirmedBarrierUpserter.new(filedate).upsert
  end
when 'Sed' then
  Dir.glob("#{BASE_FILE_PATH}/Sed/*").each do |filepath|
    filedate = filepath[/#{DATA_TYPE.downcase}(.*?).csv/, 1]
    ResultUpserter.new(filedate).upsert
  end
when 'Ukc' then
  Dir.glob("#{BASE_FILE_PATH}/Ukc/*").each do |filepath|
    filedate = filepath[/#{DATA_TYPE.downcase}(.*?).csv/, 1]
    HorseUpserter.new(filedate).upsert
  end
end
