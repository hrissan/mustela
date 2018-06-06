#include <algorithm>
#include <cassert>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <csignal>
#include "mustela.hpp"

extern "C" {
#include "blake2b.h"
}

namespace {
    typedef std::vector<uint8_t> bytes;

    auto hex_alphabet = "0123456789abcdef";

    std::string to_hex(void const* data, size_t size) {
        auto out = std::string{};
        out.reserve(32 * 2);
        for (size_t i = 0; i < size; i++) {
            auto c = static_cast<uint8_t const*>(data)[i];
            out.push_back(hex_alphabet[c >> 4]);
            out.push_back(hex_alphabet[c & 15]);
        }

        return out;
    }

    bytes from_hex(std::string const& data) {
        auto sz = data.size();
        assert(sz % 2 == 0);

        auto out = bytes{};
        out.reserve(sz / 2);

        for (size_t i = 0; i < sz; i += 2) {
            auto a = data[i];
            auto p = std::find(hex_alphabet, hex_alphabet + 16, a);
            assert(*p == a);

            auto b = data[i + 1];
            auto q = std::find(hex_alphabet, hex_alphabet + 16, b);
            assert(*q == b);

            out.push_back(static_cast<uint8_t>(((p - hex_alphabet) << 4) | (q - hex_alphabet)));
        }

        return out;
    }

    bytes encode_nulls(uint8_t tag, mustela::Val const &val) {
        auto r = bytes{};
        r.reserve(val.size + 2);

        r.push_back(tag);
        for (size_t i = 0; i < val.size; i++) {
            r.push_back(static_cast<uint8_t>(val.data[i]));
            if (val.data[i] == 0) {
                r.push_back(0xff);
            }
        }
        r.push_back(0);

        return r;
    }

    void blake2b_update_val(blake2b_ctx *ctx, uint8_t tag, mustela::Val const &val) {
        auto enc = encode_nulls(tag, val);
        blake2b_update(ctx, enc.data(), enc.size());
    }

    std::string db_hash(mustela::TX& tx) {
        auto ctx = blake2b_ctx{};
        auto ret = blake2b_init(&ctx, 32, nullptr, 0);
        assert(ret == 0);

        for (auto name: tx.get_bucket_names()) {
            blake2b_update_val(&ctx, 'b', name);

            mustela::Bucket b = tx.get_bucket(name, false);
            mustela::Cursor cur = b.get_cursor();
            mustela::Val k, v;
            for (cur.first(); cur.get(&k, &v); cur.next()) {
                blake2b_update_val(&ctx, 'k', k);
                blake2b_update_val(&ctx, 'v', v);
            }
        }

        uint8_t h[32] = {};
        blake2b_final(&ctx, &h);

        return to_hex(h, sizeof h);
    }

    struct test_state {
        std::string db_path;
        std::unique_ptr<mustela::DB> db;
        std::unique_ptr<mustela::TX> tx;
        std::map<bytes, mustela::Bucket> buckets;
        std::map<bytes, mustela::Cursor> cursors;

        explicit test_state(std::string db_path) : db_path(std::move(db_path)) {
            reset();
        }

        void commit() {
            cursors.clear();
            buckets.clear();
            if (tx) {
                tx->commit();
            }
        }

        void rollback() {
            cursors.clear();
            buckets.clear();
            if (tx) {
                tx->rollback();
            }
        }

        void reset() {
            tx = nullptr;
            db = nullptr;

            mustela::DBOptions options;
            options.new_db_page_size = mustela::MIN_PAGE_SIZE;
            options.minimal_mapping_size = 256; // Small increase of mapped region == lots of mmap/munmap when DB grows
            db = std::make_unique<mustela::DB>(db_path, options);

            tx = std::make_unique<mustela::TX>(*db, false);
        }

        static std::string get_nth_tok(std::vector<std::string> const &tokens, size_t n) {
            return tokens.size() > n ? tokens[n] : std::string{};
        }

        mustela::Bucket& obtain_bucket(bytes const& name, bool create) {
            auto it = buckets.find(name);
            if (create) {
                assert(it == buckets.end());
            }
            if (it == buckets.end()) {
                auto r = buckets.emplace(name, tx->get_bucket(mustela::Val(name), create));
                it = r.first;
            }
            return (*it).second;
        }

        void drop_bucket(bytes const& name) {
            cursors.erase(name);
            buckets.erase(name);
            tx->drop_bucket(mustela::Val(name));
        }

        mustela::Cursor& obtain_cursor(bytes const& name) {
            auto it = cursors.find(name);
            if (it == cursors.end()) {
                auto r = cursors.emplace(name, obtain_bucket(name, false).get_cursor());
                it = r.first;
            }
            return (*it).second;
        }

        std::string handle_test_command(std::vector<std::string> const &tokens) {
            auto cmd = get_nth_tok(tokens, 0);
            auto b = from_hex(get_nth_tok(tokens, 1));
            auto k = from_hex(get_nth_tok(tokens, 2));
            auto v = from_hex(get_nth_tok(tokens, 3));

            if (cmd == "create-bucket") {
                obtain_bucket(b, true);
            } else if (cmd == "drop-bucket") {
                drop_bucket(b);
            } else if (cmd == "put") {
                obtain_bucket(b, false).put(mustela::Val(k), mustela::Val(v), false);
                obtain_cursor(b).seek(mustela::Val(k));
            } else if (cmd == "put-n") {
                auto n = from_hex(get_nth_tok(tokens, 4)).at(0);
                for (uint8_t i = 0; i < n; i++) {
                    auto k_ = bytes(k);
                    auto v_ = bytes(v);
                    k_.push_back(i);
                    v_.push_back(i);
                    obtain_bucket(b, false).put(mustela::Val(k_), mustela::Val(v_), false);
                }
            } else if (cmd == "put-n-rev") {
                auto n = from_hex(get_nth_tok(tokens, 4)).at(0);
                for (int i = n - 1; i >= 0; i--) {
                    auto k_ = bytes(k);
                    auto v_ = bytes(v);
                    k_.push_back(static_cast<uint8_t>(i));
                    v_.push_back(static_cast<uint8_t>(i));
                    obtain_bucket(b, false).put(mustela::Val(k_), mustela::Val(v_), false);
                }
            } else if (cmd == "del" || cmd == "del-cursor") {
                obtain_cursor(b).seek(mustela::Val(k));
                if (cmd == "del") {
                    obtain_bucket(b, false).del(mustela::Val(k));
                } else {
                    obtain_cursor(b).del();
                }
            } else if (cmd == "del-n" || cmd == "del-n-rev") {
                auto n = v.at(0);
                auto& c = obtain_cursor(b);
                c.seek(mustela::Val(k));
                for (uint8_t i = 0; i < n; i++) {
                    c.del();
                    if (cmd == "del-n-rev") {
                        c.prev();
                    }
                }
            } else if (cmd == "commit") {
                commit();
            } else if (cmd == "rollback") {
                rollback();
            } else if (cmd == "commit-reset") {
                commit();
                reset();
            } else if (cmd == "rollback-reset") {
                rollback();
                reset();
            } else if (cmd == "kill") {
//                rollback();
                raise(SIGKILL);
            } else if (cmd == "noop") {
                return db_hash(*tx);
            } else if (cmd == "ensure-hash") {
            	std::string s1 = db_hash(*tx);
            	std::string s2 = get_nth_tok(tokens, 1);
                assert(s1 == s2);
            } else {
                std::cerr << "unknown command:" << cmd << std::endl;
            }

            tx->check_database(nullptr, false);

            return "ok";
        }
    };
}

void run_test_driver(std::string const& db_path, std::istream& scenario) {
    auto state = test_state(db_path);
    std::cerr << ">>> test (re-)start " << state.db->max_bucket_name_size() << " >>> " << state.db->max_key_size() <<  " >>>" << std::endl;

    for (std::string line; std::getline(scenario, line, '\n');) {
        std::cerr << ">>> " << line << std::endl;

        auto iss = std::istringstream(line);
        auto tokens = std::vector<std::string>{};
        for (std::string tok; std::getline(iss, tok, ',');) {
            tokens.push_back(tok);
        }

        std::cout << state.handle_test_command(tokens) << "\n";
    }
}
