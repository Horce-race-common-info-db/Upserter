class String
  def trim
    gsub(/(^[[:space:]]+)|([[:space:]]+$)/, '')
  end
end

class Upserter
  BASE_FILE_PATH = ENV['CONVERT_FILE_OUTPUT_DIRECTORY'].freeze
  FILE_EXTENTION  = '.csv'.freeze
end
