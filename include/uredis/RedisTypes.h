#ifndef REDISTYPES_H
#define REDISTYPES_H

#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
#include <variant>
#include <vector>
#include <unordered_map>
#include <map>
#include <optional>
#include <utility>
#include <charconv>

namespace usub::uredis
{
    enum class RedisType
    {
        Null,
        SimpleString,
        Error,
        Integer,
        BulkString,
        Array
    };

    struct RedisValue
    {
        using Array = std::vector<RedisValue>;

        RedisType type{RedisType::Null};
        std::variant<std::monostate, std::string, int64_t, Array> value;

        [[nodiscard]] bool is_null() const { return this->type == RedisType::Null; }
        [[nodiscard]] bool is_error() const { return this->type == RedisType::Error; }
        [[nodiscard]] bool is_simple_string() const { return this->type == RedisType::SimpleString; }
        [[nodiscard]] bool is_bulk_string() const { return this->type == RedisType::BulkString; }
        [[nodiscard]] bool is_integer() const { return this->type == RedisType::Integer; }
        [[nodiscard]] bool is_array() const { return this->type == RedisType::Array; }

        [[nodiscard]] const std::string& as_string() const
        {
            return std::get<std::string>(this->value);
        }

        [[nodiscard]] int64_t as_integer() const
        {
            return std::get<int64_t>(this->value);
        }

        [[nodiscard]] const Array& as_array() const
        {
            return std::get<Array>(this->value);
        }

        [[nodiscard]] std::map<std::string, std::string> as_map() const
        {
            std::map<std::string, std::string> result;

            if (!this->is_array()) return result;
            const auto& arr = this->as_array();

            if (arr.size() % 2 != 0) return result;

            for (size_t i = 0; i < arr.size(); i += 2)
            {
                const auto& fk = arr[i];
                const auto& fv = arr[i + 1];

                if (!(fk.is_simple_string() || fk.is_bulk_string())) continue;
                if (!(fv.is_simple_string() || fv.is_bulk_string())) continue;

                result.emplace(fk.as_string(), fv.as_string());
            }

            return result;
        }

        [[nodiscard]] std::unordered_map<std::string, std::string> as_unordered_map() const
        {
            std::unordered_map<std::string, std::string> result;

            if (!this->is_array()) return result;
            const auto& arr = this->as_array();

            if (arr.size() % 2 != 0) return result;

            result.reserve(arr.size() / 2);

            for (size_t i = 0; i < arr.size(); i += 2)
            {
                const auto& fk = arr[i];
                const auto& fv = arr[i + 1];

                if (!(fk.is_simple_string() || fk.is_bulk_string())) continue;
                if (!(fv.is_simple_string() || fv.is_bulk_string())) continue;

                result.emplace(fk.as_string(), fv.as_string());
            }

            return result;
        }

        [[nodiscard]] std::vector<std::string> as_string_array() const
        {
            std::vector<std::string> out;

            if (!this->is_array()) return out;
            const auto& arr = this->as_array();

            out.reserve(arr.size());
            for (const auto& v : arr)
            {
                if (v.is_simple_string() || v.is_bulk_string())
                    out.push_back(v.as_string());
            }

            return out;
        }

        [[nodiscard]] std::vector<std::pair<std::string, std::string>> as_vector_pairs() const
        {
            std::vector<std::pair<std::string, std::string>> out;

            if (!this->is_array()) return out;
            const auto& arr = this->as_array();

            if (arr.size() % 2 != 0) return out;

            out.reserve(arr.size() / 2);

            for (size_t i = 0; i < arr.size(); i += 2)
            {
                const auto& fk = arr[i];
                const auto& fv = arr[i + 1];

                if (!(fk.is_simple_string() || fk.is_bulk_string())) continue;
                if (!(fv.is_simple_string() || fv.is_bulk_string())) continue;

                out.emplace_back(fk.as_string(), fv.as_string());
            }

            return out;
        }

        [[nodiscard]] std::optional<std::string> as_optional_string() const
        {
            if (this->is_null())
                return std::nullopt;

            if (this->is_simple_string() || this->is_bulk_string())
                return this->as_string();

            return std::nullopt;
        }

        [[nodiscard]] std::optional<int64_t> as_optional_integer() const
        {
            if (this->is_null())
                return std::nullopt;

            if (this->is_integer())
                return this->as_integer();

            if (this->is_simple_string() || this->is_bulk_string())
            {
                const auto& s = this->as_string();
                int64_t v{};
                auto* begin = s.data();
                auto* end = s.data() + s.size();
                auto [p, ec] = std::from_chars(begin, end, v);
                if (ec == std::errc{} && p == end)
                    return v;
            }

            return std::nullopt;
        }
    };

    enum class RedisErrorCategory
    {
        Io,
        Protocol,
        ServerReply,
        Timeout
    };

    struct RedisError
    {
        RedisErrorCategory category{RedisErrorCategory::Protocol};
        std::string message;
    };

    template <typename T>
    using RedisResult = std::expected<T, RedisError>;
} // namespace usub::uredis

#endif //REDISTYPES_H
