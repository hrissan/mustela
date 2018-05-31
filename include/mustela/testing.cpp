#include <algorithm>
#include <cassert>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include "mustela.hpp"

extern "C" {
#include "blake2b.h"
}

namespace {
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

    std::vector<uint8_t> from_hex(std::string const& data) {
        auto sz = data.size();
        assert(sz % 2 == 0);

        auto out = std::vector<uint8_t>{};
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

    std::vector<uint8_t> encode_nulls(uint8_t tag, mustela::Val const &val) {
        auto r = std::vector<uint8_t>{};
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

            mustela::Bucket b(tx, name, false);
            mustela::Cursor cur(b);
            mustela::Val k, v;
            for (cur.first(); cur.get(k, v); cur.next()) {
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

        explicit test_state(std::string db_path): db_path(std::move(db_path)) {
            reset(false);
        }

        void rollback() {
            tx = nullptr;
            tx = std::make_unique<mustela::TX>(*db, false);
        }

        void reset(bool commit) {
            if (tx && commit) {
                tx->commit();
            }

            tx = nullptr;
            db = nullptr;
            db = std::make_unique<mustela::DB>(db_path, false);
            tx = std::make_unique<mustela::TX>(*db, false);
        }
    };

    std::string get_nth_tok(std::vector<std::string> const& tokens, size_t n) {
        return tokens.size() > n ? tokens[n] : std::string{};
    }

    std::string handle_test_command(test_state& state, std::vector<std::string> const& tokens) {
        auto cmd = get_nth_tok(tokens, 0);

        if (cmd == "create-bucket") {
            auto name = from_hex(get_nth_tok(tokens, 1));
            mustela::Bucket b(*state.tx, mustela::Val(name.data(), name.size()), true);
        } else if (cmd == "drop-bucket") {
            auto name = from_hex(get_nth_tok(tokens, 1));
            state.tx->drop_bucket(mustela::Val(name.data(), name.size()));
        } else if (cmd == "put") {
            auto name = from_hex(get_nth_tok(tokens, 1));
            mustela::Bucket b(*state.tx, mustela::Val(name.data(), name.size()), false);
            auto k = from_hex(get_nth_tok(tokens, 2));
            auto v = from_hex(get_nth_tok(tokens, 3));
            b.put(mustela::Val(k.data(), k.size()), mustela::Val(v.data(), v.size()), false);
        } else if (cmd == "del") {
            auto name = from_hex(get_nth_tok(tokens, 1));
            mustela::Bucket b(*state.tx, mustela::Val(name.data(), name.size()), false);
            auto k = from_hex(get_nth_tok(tokens, 2));
            b.del(mustela::Val(k.data(), k.size()), true);
        } else if (cmd == "commit") {
            state.tx->commit();
        } else if (cmd == "rollback") {
            state.rollback();
        } else if (cmd == "commit-reset") {
            state.reset(true);
        } else if (cmd == "rollback-reset") {
            state.reset(false);
        } else {
            std::cerr << "unknown command:" << cmd << std::endl;
        }

        return db_hash(*state.tx);
    }
}

void run_test_driver(std::string const& db_path) {
    auto state = test_state(db_path);

    for (std::string line; std::getline(std::cin, line, '\n');) {
        auto iss = std::istringstream(line);
        auto tokens = std::vector<std::string>{};
        for (std::string tok; std::getline(iss, tok, ',');) {
            tokens.push_back(tok);
        }

        std::cout << handle_test_command(state, tokens) << "\n";
    }
}
