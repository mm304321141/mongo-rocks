/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/base/status.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/constraints.h"

#include "rocks_global_options.h"

namespace mongo {

    RocksGlobalOptions rocksGlobalOptions;

    Status RocksGlobalOptions::add(moe::OptionSection* options) {
        moe::OptionSection rocksOptions("RocksDB options");

        rocksOptions.addOptionChaining("storage.rocksdb.cacheSizeGB", "rocksdbCacheSizeGB",
                                       moe::Int,
                                       "maximum amount of memory to allocate for cache; "
                                       "defaults to 30%% of physical RAM").validRange(1, 10000);
        rocksOptions.addOptionChaining("storage.rocksdb.compression", "rocksdbCompression",
                                       moe::String,
                                       "block compression algorithm for collection data "
                                       "[none|snappy|zlib|lz4|lz4hc]")
            .format("(:?none)|(:?snappy)|(:?zlib)|(:?lz4)|(:?lz4hc)", "(none/snappy/zlib/lz4/lz4hc)")
            .setDefault(moe::Value(std::string("snappy")));
        rocksOptions
            .addOptionChaining(
                 "storage.rocksdb.maxWriteMBPerSec", "rocksdbMaxWriteMBPerSec", moe::Int,
                 "Maximum speed that RocksDB will write to storage. Reducing this can "
                 "help reduce read latency spikes during compactions. However, reducing this "
                 "below a certain point might slow down writes. Defaults to 1GB/sec")
            .validRange(1, 1024)
            .setDefault(moe::Value(1024));
        rocksOptions.addOptionChaining("storage.rocksdb.configString", "rocksdbConfigString",
                                       moe::String,
                                       "RocksDB storage engine custom "
                                       "configuration settings").hidden();
        rocksOptions.addOptionChaining("storage.rocksdb.crashSafeCounters",
                                       "rocksdbCrashSafeCounters", moe::Bool,
                                       "If true, numRecord and dataSize counter will be consistent "
                                       "even after power failure. If false, numRecord and dataSize "
                                       "might be a bit inconsistent after power failure, but "
                                       "should be correct under normal conditions. Setting this to "
                                       "true will make database inserts a bit slower.")
            .setDefault(moe::Value(false))
            .hidden();

        rocksOptions
            .addOptionChaining("storage.rocksdb.counters",
                               "rocksdbCounters",
                               moe::Bool,
                               "If true, we will turn on RocksDB's advanced counters")
            .setDefault(moe::Value(true));
        rocksOptions
            .addOptionChaining("storage.rocksdb.singleDeleteIndex",
                               "rocksdbSingleDeleteIndex", moe::Bool,
                               "This is still experimental. "
                               "Use this only if you know what you're doing")
            .setDefault(moe::Value(false));

        // terark begin

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.indexNestLevel",
                                   "terarkIndexNestLevel",
                                   moe::Int,
                                   "Index nest level.")
                .validRange(1, 10)
                .setDefaylt(moe::Value(3))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.checksumLevel",
                                   "terarkChecksumLevel",
                                   moe::Int,
                                   "Case 0, check sum nothing. "
                                   "case 1, check sum meta data and index, check on file load. "
                                   "case 2, check sum all data, not check on file load, check on record read. "
                                   "case 3, check sum all data with one checksum value, not checksum each record, "
                                   "if checksum doesn't match, load will fail")
                .validRange(0, 3)
                .setDefaylt(moe::Value(1));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.entropyAlgo",
                                   "terarkEntropyAlgo",
                                   moe::String,
                                   "Entropy algo. "
                                   "[none|huffman|FSE]")
                .format("(:?none)|(:?huffman)|(:?FSE)", "(none/huffman/FSE)")
                .setDefaylt(moe::Value(std::string("none")))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.terarkZipMinLevel",
                                   "terarkZipMinLevel",
                                   moe::Int,
                                   "When value < 0, only last level uses terarkZip. "
                                   "This is equivalent to when terarkZipMinLevel == num_levels-1. "
                                   "In other scenarios , use terarkZip when curlevel >= terarkZipMinLevel. "
                                   "This includes two special cases: "
                                   "(1) when value == 0, all levels use terarkZip; "
                                   "(2) when value >= num levels, all levels use fallback TableFactory. "
                                   "It shown that terarkZipMinLevel = 0 is the best choice. "
                                   "If mixed with rocksdb's native SST, "
                                   "those SSTs may use too much memory & SSD, "
                                   "which degrades the performance.")
                .setDefaylt(moe::Value(0));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.useSuffixArrayLocalMatch",
                                   "terarkUseSuffixArrayLocalMatch",
                                   moe::Bool,
                                   "Use suffix array local match.")
                .setDefaylt(moe::Value(false))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.warmUpIndexOnOpen",
                                   "terarkWarmUpIndexOnOpen",
                                   moe::Bool,
                                   "Warm up index on open.")
                .setDefaylt(moe::Value(true))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.warmUpValueOnOpen",
                                   "terarkWarmUpValueOnOpen",
                                   moe::Bool,
                                   "Warm up value on open.")
                .setDefaylt(moe::Value(false))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.estimateCompressionRatio",
                                   "terarkEstimateCompressionRatio",
                                   moe::Double,
                                   "To let rocksdb compaction algo know the estimate SST file size")
                .setDefaylt(moe::Value(0.2))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.sampleRatio",
                                   "terarkSampleRatio",
                                   moe::Double,
                                   "The global dictionary size over all value size")
                .setDefaylt(moe::Value(0.03))
                .hidden();

        rocksOptions
            .addOptionChaining("storage.rocksdb.terark.localTempDir",
                               "terarkLocalTempDir",
                               moe::String,
                               "TerarkZipTable needs to create temp files during compression")
            .setDefaylt(moe::Value(std::string("/tmp")));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.indexType",
                                   "terarkIndexType",
                                   moe::String,
                                   "Index rank select type. ")
                .format("(:?NestLoudsTrieDAWG_IL)|(:?NestLoudsTrieDAWG_IL_256)|(:?IL_256_32)|"
                        "(:?NestLoudsTrieDAWG_Mixed_IL_256)|(:?Mixed_IL_256)|(:?NestLoudsTrieDAWG_Mixed_SE_512)|"
                        "(:?Mixed_SE_512)|(:?NestLoudsTrieDAWG_Mixed_XL_256)|(:?Mixed_XL_256)|"
                        "(:?NestLoudsTrieDAWG_SE_512)|(:?SE_512)|(:?SE_512_32)|(:?IL_256)",
                        "(SE_512/IL_256/Mixed_SE_512/Mixed_IL_256/Mixed_XL_256)")
                .setDefaylt(moe::Value(std::string("IL_256")))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.softZipWorkingMemLimit",
                                   "terarkSoftMemLimit",
                                   moe::UnsignedLongLong,
                                   "Soft zip working memory limit")
                .setDefaylt(moe::Value(16ull << 30));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.hardZipWorkingMemLimit",
                                   "terarkHardMemLimit",
                                   moe::UnsignedLongLong,
                                   "Hard zip working memory limit")
                .setDefaylt(moe::Value(32ull << 30));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.smallTaskMemory",
                                   "terarkSmallTaskMemory",
                                   moe::UnsignedLongLong,
                                   "Small task memory size")
                .setDefaylt(moe::Value(1200ull << 20))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terark.indexCacheRatio",
                                   "terarkIndexCacheRatio",
                                   moe::Double,
                                   "Index cache ratio, should be a small value, typically 0.001. "
                                   "default is to disable indexCache, because the improvement. "
                                   "is about only 10% when set to 0.001")
                .setDefaylt(moe::Value(0.0))
                .hidden();

        // terark end

        return options->addSection(rocksOptions);
    }

    Status RocksGlobalOptions::store(const moe::Environment& params,
                                     const std::vector<std::string>& args) {
        if (params.count("storage.rocksdb.cacheSizeGB")) {
            rocksGlobalOptions.cacheSizeGB = params["storage.rocksdb.cacheSizeGB"].as<int>();
            log() << "Block Cache Size GB: " << rocksGlobalOptions.cacheSizeGB;
        }
        if (params.count("storage.rocksdb.compression")) {
            rocksGlobalOptions.compression =
                params["storage.rocksdb.compression"].as<std::string>();
            log() << "Compression: " << rocksGlobalOptions.compression;
        }
        if (params.count("storage.rocksdb.maxWriteMBPerSec")) {
            rocksGlobalOptions.maxWriteMBPerSec =
                params["storage.rocksdb.maxWriteMBPerSec"].as<int>();
            log() << "MaxWriteMBPerSec: " << rocksGlobalOptions.maxWriteMBPerSec;
        }
        if (params.count("storage.rocksdb.configString")) {
            rocksGlobalOptions.configString =
                params["storage.rocksdb.configString"].as<std::string>();
            log() << "Engine custom option: " << rocksGlobalOptions.configString;
        }
        if (params.count("storage.rocksdb.crashSafeCounters")) {
            rocksGlobalOptions.crashSafeCounters =
                params["storage.rocksdb.crashSafeCounters"].as<bool>();
            log() << "Crash safe counters: " << rocksGlobalOptions.crashSafeCounters;
        }
        if (params.count("storage.rocksdb.counters")) {
            rocksGlobalOptions.counters =
              params["storage.rocksdb.counters"].as<bool>();
            log() << "Counters: " << rocksGlobalOptions.counters;
        }
        if (params.count("storage.rocksdb.singleDeleteIndex")) {
            rocksGlobalOptions.singleDeleteIndex =
              params["storage.rocksdb.singleDeleteIndex"].as<bool>();
            log() << "Use SingleDelete in index: " << rocksGlobalOptions.singleDeleteIndex;
        }

        //terark begin
        if (params.count("storage.rocksdb.terark.indexNestLevel")) {
            rocksGlobalOptions.terarkOptions.indexNestLevel =
                    params["storage.rocksdb.terark.indexNestLevel"].as<int>();
            log() << "Terark IndexNestLevel: " << rocksGlobalOptions.terarkOptions.indexNestLevel;
        }
        if (params.count("storage.rocksdb.terark.checksumLevel")) {
            rocksGlobalOptions.terarkOptions.checksumLevel =
                    params["storage.rocksdb.terark.checksumLevel"].as<int>();
            log() << "Terark ChecksumLevel: " << rocksGlobalOptions.terarkOptions.checksumLevel;
        }
        if (params.count("storage.rocksdb.terark.entropyAlgo")) {
            rocksGlobalOptions.terarkOptions.entropyAlgo =
                    params["storage.rocksdb.terark.entropyAlgo"].as<std::string>();
            log() << "Terark EntropyAlgo: " << rocksGlobalOptions.terarkOptions.entropyAlgo;
        }
        if (params.count("storage.rocksdb.terark.terarkZipMinLevel")) {
            rocksGlobalOptions.terarkOptions.terarkZipMinLevel =
                    params["storage.rocksdb.terark.terarkZipMinLevel"].as<int>();
            log() << "Terark TerarkZipMinLevel: " << rocksGlobalOptions.terarkOptions.terarkZipMinLevel;
        }
        if (params.count("storage.rocksdb.terark.useSuffixArrayLocalMatch")) {
            rocksGlobalOptions.terarkOptions.useSuffixArrayLocalMatch =
                    params["storage.rocksdb.terark.useSuffixArrayLocalMatch"].as<bool>();
            log() << "Terark UseSuffixArrayLocalMatch: " << rocksGlobalOptions.terarkOptions.useSuffixArrayLocalMatch;
        }
        if (params.count("storage.rocksdb.terark.warmUpIndexOnOpen")) {
            rocksGlobalOptions.terarkOptions.warmUpIndexOnOpen =
                    params["storage.rocksdb.terark.warmUpIndexOnOpen"].as<bool>();
            log() << "Terark WarmUpIndexOnOpen: " << rocksGlobalOptions.terarkOptions.warmUpIndexOnOpen;
        }
        if (params.count("storage.rocksdb.terark.warmUpValueOnOpen")) {
            rocksGlobalOptions.terarkOptions.warmUpValueOnOpen =
                    params["storage.rocksdb.terark.warmUpValueOnOpen"].as<bool>();
            log() << "Terark WarmUpValueOnOpen: " << rocksGlobalOptions.terarkOptions.warmUpValueOnOpen;
        }
        if (params.count("storage.rocksdb.terark.estimateCompressionRatio")) {
            rocksGlobalOptions.terarkOptions.estimateCompressionRatio =
                    params["storage.rocksdb.terark.estimateCompressionRatio"].as<double>();
            log() << "Terark EstimateCompressionRatio: " << rocksGlobalOptions.terarkOptions.estimateCompressionRatio;
        }
        if (params.count("storage.rocksdb.terark.sampleRatio")) {
            rocksGlobalOptions.terarkOptions.sampleRatio =
                    params["storage.rocksdb.terark.sampleRatio"].as<double>();
            log() << "Terark SampleRatio: " << rocksGlobalOptions.terarkOptions.sampleRatio;
        }
        if (params.count("storage.rocksdb.terark.localTempDir")) {
            rocksGlobalOptions.terarkOptions.localTempDir =
                    params["storage.rocksdb.terark.localTempDir"].as<std::string>();
            log() << "Terark LocalTempDir: " << rocksGlobalOptions.terarkOptions.localTempDir;
        }
        if (params.count("storage.rocksdb.terark.indexType")) {
            rocksGlobalOptions.terarkOptions.indexType =
                    params["storage.rocksdb.terark.indexType"].as<std::string>();
            log() << "Terark IndexType: " << rocksGlobalOptions.terarkOptions.indexType;
        }
        if (params.count("storage.rocksdb.terark.softZipWorkingMemLimit")) {
            rocksGlobalOptions.terarkOptions.softZipWorkingMemLimit =
                    params["storage.rocksdb.terark.softZipWorkingMemLimit"].as<uint64_t>();
            log() << "Terark SoftZipWorkingMemLimit: " << rocksGlobalOptions.terarkOptions.softZipWorkingMemLimit;
        }
        if (params.count("storage.rocksdb.terark.hardZipWorkingMemLimit")) {
            rocksGlobalOptions.terarkOptions.hardZipWorkingMemLimit =
                    params["storage.rocksdb.terark.hardZipWorkingMemLimit"].as<uint64_t>();
            log() << "Terark HardZipWorkingMemLimit: " << rocksGlobalOptions.terarkOptions.hardZipWorkingMemLimit;
        }
        if (params.count("storage.rocksdb.terark.smallTaskMemory")) {
            rocksGlobalOptions.terarkOptions.smallTaskMemory =
                    params["storage.rocksdb.terark.smallTaskMemory"].as<uint64_t>();
            log() << "Terark SmallTaskMemory: " << rocksGlobalOptions.terarkOptions.smallTaskMemory;
        }
        if (params.count("storage.rocksdb.terark.indexCacheRatio")) {
            rocksGlobalOptions.terarkOptions.indexCacheRatio =
                    params["storage.rocksdb.terark.indexCacheRatio"].as<double>();
            log() << "Terark IndexCacheRatio: " << rocksGlobalOptions.terarkOptions.indexCacheRatio;
        }
        //terark end

        return Status::OK();
    }

}  // namespace mongo
