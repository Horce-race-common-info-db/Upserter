require './upserters/horse_upserter.rb'
require './upserters/jockey_upserter.rb'
require './upserters/race_upserter.rb'
require './upserters/run_upserter.rb'
require './upserters/confirmed_barrier_upserter.rb'

HorseUpserter.new(ARGV[0]).upsert
JockeyUpserter.new(ARGV[0]).upsert
RaceUpserter.new(ARGV[0]).upsert
RunUpserter.new(ARGV[0]).upsert
ConfirmedBarrierUpserter.new(ARGV[0]).upsert
