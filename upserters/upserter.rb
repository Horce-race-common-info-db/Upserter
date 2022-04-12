class String
  def trim
    gsub(/(^[[:space:]]+)|([[:space:]]+$)/, '')
  end
end

class Upserter
  BASE_FILE_PATH = 'downloader/output/'.freeze
  FILE_EXTENTION  = '.csv'.freeze
end
