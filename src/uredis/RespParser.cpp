#include "uredis/RespParser.h"

#include <charconv>
#include <stdexcept>

namespace usub::uredis
{
    void RespParser::reset()
    {
        this->buffer_.clear();
        this->buffer_.shrink_to_fit();
        this->pos_ = 0;
    }

    void RespParser::feed(const uint8_t* data, std::size_t len)
    {
        if (len == 0) return;
        this->buffer_.insert(this->buffer_.end(), data, data + len);
    }

    bool RespParser::ensure(std::size_t n) const
    {
        return this->pos_ + n <= this->buffer_.size();
    }

    std::optional<std::size_t> RespParser::find_crlf(std::size_t from) const
    {
        for (std::size_t i = from; i + 1 < this->buffer_.size(); ++i)
        {
            if (this->buffer_[i] == '\r' && this->buffer_[i + 1] == '\n') return i;
        }
        return std::nullopt;
    }

    std::optional<std::string> RespParser::read_line()
    {
        auto crlf_pos_opt = this->find_crlf(this->pos_);
        if (!crlf_pos_opt) return std::nullopt;

        std::size_t end = *crlf_pos_opt;
        if (end < this->pos_) return std::nullopt;

        std::string line(
            reinterpret_cast<const char*>(&this->buffer_[this->pos_]),
            end - this->pos_);

        this->pos_ = end + 2; // skip \r\n
        return line;
    }

    void RespParser::compact_if_needed()
    {
        if (this->pos_ > 0 && this->pos_ > this->buffer_.size() / 2)
        {
            std::vector<uint8_t> tmp(this->buffer_.begin() + static_cast<std::ptrdiff_t>(this->pos_),
                                     this->buffer_.end());
            this->buffer_.swap(tmp);
            this->pos_ = 0;
        }
    }

    std::optional<RedisValue> RespParser::next()
    {
        if (!this->ensure(1)) return std::nullopt;

        // A reply may arrive split across several reads. The parse_* helpers advance pos_
        // as they go, so an incomplete value must rewind to its first byte: otherwise the
        // next call resumes in the middle of the payload and the stream is desynchronised
        // (the caller then waits for bytes that never come, until its IO timeout).
        // The buffer is compacted only here, after a complete value, so the rewind
        // position stays valid.
        const std::size_t start = this->pos_;
        auto value = this->parse_value();
        if (!value)
        {
            this->pos_ = start;
            return std::nullopt;
        }
        compact_if_needed();
        return value;
    }

    std::optional<RedisValue> RespParser::parse_value()
    {
        if (!this->ensure(1)) return std::nullopt;

        char prefix = static_cast<char>(this->buffer_[this->pos_]);
        switch (prefix)
        {
        case '+':
            return this->parse_simple_string();
        case '-':
            return this->parse_error();
        case ':':
            return this->parse_integer();
        case '$':
            return this->parse_bulk_string();
        case '*':
            return this->parse_array();
        default:
            return std::nullopt;
        }
    }

    std::optional<RedisValue> RespParser::parse_simple_string()
    {
        ++this->pos_; // skip '+'
        auto line_opt = this->read_line();
        if (!line_opt) return std::nullopt;

        RedisValue v;
        v.type  = RedisType::SimpleString;
        v.value = std::move(*line_opt);
        return v;
    }

    std::optional<RedisValue> RespParser::parse_error()
    {
        ++this->pos_; // skip '-'
        auto line_opt = this->read_line();
        if (!line_opt) return std::nullopt;

        RedisValue v;
        v.type  = RedisType::Error;
        v.value = std::move(*line_opt);
        return v;
    }

    std::optional<RedisValue> RespParser::parse_integer()
    {
        ++this->pos_; // skip ':'
        auto line_opt = this->read_line();
        if (!line_opt) return std::nullopt;

        const std::string& line = *line_opt;
        int64_t value{};
        auto [ptr, ec] = std::from_chars(line.data(), line.data() + line.size(), value);
        if (ec != std::errc{})
        {
            return std::nullopt;
        }

        RedisValue v;
        v.type  = RedisType::Integer;
        v.value = value;
        return v;
    }

    std::optional<RedisValue> RespParser::parse_bulk_string()
    {
        ++this->pos_; // skip '$'
        auto line_opt = this->read_line();
        if (!line_opt) return std::nullopt;

        const std::string& line = *line_opt;
        long long len{};
        auto [ptr, ec] = std::from_chars(line.data(), line.data() + line.size(), len);
        if (ec != std::errc{}) return std::nullopt;

        if (len < 0)
        {
            // Null bulk string
            RedisValue v;
            v.type  = RedisType::Null;
            v.value = std::monostate{};
            return v;
        }

        std::size_t ulen = static_cast<std::size_t>(len);
        if (!this->ensure(ulen + 2)) return std::nullopt; // data + \r\n

        std::string s(
            reinterpret_cast<const char*>(&this->buffer_[this->pos_]),
            ulen);

        this->pos_ += ulen;
        if (!this->ensure(2)) return std::nullopt;
        // skip \r\n
        this->pos_ += 2;

        RedisValue v;
        v.type  = RedisType::BulkString;
        v.value = std::move(s);
        return v;
    }

    std::optional<RedisValue> RespParser::parse_array()
    {
        ++this->pos_; // skip '*'
        auto line_opt = this->read_line();
        if (!line_opt) return std::nullopt;

        const std::string& line = *line_opt;
        long long len{};
        auto [ptr, ec] = std::from_chars(line.data(), line.data() + line.size(), len);
        if (ec != std::errc{}) return std::nullopt;

        if (len < 0)
        {
            RedisValue v;
            v.type  = RedisType::Null;
            v.value = std::monostate{};
            return v;
        }

        std::size_t ulen = static_cast<std::size_t>(len);
        RedisValue::Array arr;
        arr.reserve(ulen);

        for (std::size_t i = 0; i < ulen; ++i)
        {
            auto elem_opt = this->parse_value();
            if (!elem_opt) return std::nullopt;
            arr.push_back(std::move(*elem_opt));
        }

        RedisValue v;
        v.type  = RedisType::Array;
        v.value = std::move(arr);
        return v;
    }
} // namespace usub::uredis