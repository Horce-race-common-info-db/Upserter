require_relative 'upserters/horse_upserter.rb'
require_relative 'upserters/jockey_upserter.rb'
require_relative 'upserters/race_upserter.rb'
require_relative 'upserters/run_upserter.rb'
require_relative 'upserters/confirmed_barrier_upserter.rb'

HorseUpserter.new(ARGV[0]).upsert
JockeyUpserter.new(ARGV[0]).upsert
RaceUpserter.new(ARGV[0]).upsert
RunUpserter.new(ARGV[0]).upsert
ConfirmedBarrierUpserter.new(ARGV[0]).upsert
