require './upserters/horse_upserter.rb'

# 馬基本データ: Ukc
DATA_TYPE = ARGV[0]

Dir.glob("./downloader/output/#{DATA_TYPE}/*").each do |filepath|
  filedate = filepath[/#{DATA_TYPE.downcase}(.*?).txt/, 1]

  case DATA_TYPE
  when 'Ukc' then
    HorseUpserter.new(filedate).upsert
  end
end
