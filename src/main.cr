require "uri"
require "redis"


def help
  abort "Usage: rsmerge <source> <destination>"
  exit 1
end


latest = "0"
src_url = ARGV[0]? || ""
dst_url = ARGV[1]? || ""

help if src_url.blank? || dst_url.blank?


def sort_by_ids(ids)
  ids.sort do |a,b|
    a1, a2 = a.split("-").map(&.to_u64)
    b1, b2 = b.split("-").map(&.to_u64)

    res = a1 <=> b1
    # puts "a1=#{a1} <=> b1=#{b1}        a2=#{a2} <=> b2=#{b2}    => #{res}"

    if res.zero?
      a2 <=> b2
    else
      res
    end
  end
end

class Conn
  @key : String
  getter :url, :key, :redis

  def self.url_to_key(url)
    path = URI.parse(url).path || ""
    path = path.sub(/^\/\d+\//, "/") # strip redis db selection
    path = path.sub(/^\//, "")       # strip leading /
  end

  def self.next_key(key : String)
    a, b = key.split("-")
    "#{a}-#{b.to_u64 + 1}"
  end


  def initialize(url : String)
    @url   = url
    @key   = self.class.url_to_key url
    @redis = Redis.new(url: redis_url)
  end

  def redis_url
    url.chomp(key).sub(/\/$/, "")
  end

  def exists?
    redis.exists(key).to_i > 0
  end

  def type
    redis.type(key) if exists?
  end
end

class Stream
  getter :conn

  def initialize(conn : Conn)
    @conn = conn
  end

  def add(id : String, entry : String)
    conn.redis.xadd conn.key, {"order" => entry}, id: id
  end

  def each
    latest = "0"
    while (entries = conn.redis.xrange(conn.key, latest, "+", count: 1_000)) && !entries.empty?
      entries.each do |(id, entry)|
        yield id, entry["order"].as(String)
      end
      latest = Conn.next_key(entries.keys.last.as(String))
    end
  end

  def count
    conn.redis.xlen(conn.key)
  end
end

class Hashset
  getter :conn

  def initialize(conn : Conn)
    @conn = conn
  end

  def key
    conn.key
  end

  def ids_key
    key + ":ids"
  end

  def add(id : String, entry : String)
    conn.redis.hset key, id, entry
    conn.redis.sadd ids_key, id
  end

  def each
    ids = conn.redis.smembers(ids_key).map(&.to_s)
    ids = sort_by_ids(ids)

    ids.each do |id|
      if entry = conn.redis.hget(key, id)
        yield id, entry.as(String)
      end
    end
  end

  def count
    res = conn.redis.hlen(key)
    raise "Error: hash and set have different counts" if res != conn.redis.scard(ids_key)
    res
  end
end


src_conn = Conn.new(src_url)
dst_conn = Conn.new(dst_url)

src = Stream.new src_conn
dst = Hashset.new dst_conn

case src_conn.type
when "stream"
  case dst_conn.type
  when "hash", nil
    puts "src=stream dst=hashset"
    # all good: read from stream write to hashset
  else
    abort "Stream to #{dst_conn.type.inspect} not supported."
  end
when "hash"
  case dst_conn.type
  when "stream", nil
    # all good: read from hashset write to stream
    puts "src=hashset dst=stream"
    src = Hashset.new src_conn
    dst = Stream.new dst_conn
  else
    abort "Hashset to #{dst_conn.type.inspect} not supported."
  end
when nil
  abort "Source stream does not exist. src_key=#{src_conn.key}"
end


# Copy entries
src.each do |id, entry|
  dst.add id, entry
end

sc, dc = src.count, dst.count
if sc == dc
  print "OK "
else
  print "ERROR "
end
puts "src=#{sc} == dst=#{dc}"
exit (dc - sc).to_i
