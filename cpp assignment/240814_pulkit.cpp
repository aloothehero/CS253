#include <bits/stdc++.h>
using namespace std;

// -------------------------------
// Custom exception type
// -------------------------------
class AnalyzerException : public std::runtime_error {
public:
    explicit AnalyzerException(const string &msg) : runtime_error(msg) {}
};

// -------------------------------
// Template: input validation
// (user-defined template requirement)
// -------------------------------
template <typename T>
void validateRange(T value, T min_val, T max_val, const string &errMsg) {
    if (value < min_val || value > max_val) throw AnalyzerException(errMsg);
}

// -------------------------------
// BufferedReader
// - Uses C stdio fread for fast, predictable chunked reading.
// - Buffer size is fixed (KB) and constant during execution.
// -------------------------------
class BufferedReader {
private:
    FILE *fp;
    size_t bufBytes;
    vector<char> buffer;

public:
    BufferedReader(const string &path, size_t bufferKB)
        : fp(nullptr), bufBytes(bufferKB * 1024), buffer(bufBytes)
    {
        fp = fopen(path.c_str(), "rb");
        if (!fp) {
            throw AnalyzerException("Cannot open file: " + path);
        }
    }

    // read up to bufBytes into out (resize out to n read); returns n read (0 => EOF)
    size_t readChunk(vector<char> &out) {
        out.resize(bufBytes);
        size_t got = fread(out.data(), 1, bufBytes, fp);
        if (got == 0) {
            out.clear();
            return 0;
        }
        out.resize(got);
        return got;
    }

    ~BufferedReader() {
        if (fp) fclose(fp);
    }
};

// -------------------------------
// Tokenizer
// - Extracts contiguous alphanumeric tokens (A-Z a-z 0-9)
// - Converts letters to lowercase (case-insensitive matching)
// - Handles tokens split across buffer boundaries via leftover string
// - Provides two APIs:
//     1) tokenizeToMap(chunk, map)  -> directly increments map counts (avoids temporaries)
//     2) tokenizeToVector(chunk)    -> returns vector<string> (not used in hot path but available)
// -------------------------------
class Tokenizer {
private:
    string leftover; // partial token carried across chunk boundaries

    inline char lowerAscii(char c) {
        if (c >= 'A' && c <= 'Z') return c + ('a' - 'A');
        return c;
    }

    inline bool isAlnumAscii(unsigned char c) {
        // Only ASCII alnum considered as per dataset assumption
        return ( (c >= '0' && c <= '9')
              || (c >= 'A' && c <= 'Z')
              || (c >= 'a' && c <= 'z') );
    }

public:
    Tokenizer() : leftover() {}

    // Process a chunk and increment counts directly into freq map (fast path)
    void tokenizeToMap(const vector<char> &chunk, unordered_map<string,long long> &freq) {
        string current = std::move(leftover);
        leftover.clear();

        for (size_t i = 0; i < chunk.size(); ++i) {
            unsigned char uc = static_cast<unsigned char>(chunk[i]);
            if (isAlnumAscii(uc)) {
                if (uc >= 'A' && uc <= 'Z') current.push_back(lowerAscii(chunk[i]));
                else current.push_back(chunk[i]); // digits and lowercase letters as-is
            } else {
                if (!current.empty()) {
                    ++freq[current];
                    current.clear();
                }
            }
        }
        // If there's a partial token at the end of the chunk, keep it as leftover
        leftover = std::move(current);
    }

    // Tokenize chunk and return vector (safer/clearer path; not used for very large files)
    vector<string> tokenizeToVector(const vector<char> &chunk) {
        vector<string> out;
        string current = std::move(leftover);
        leftover.clear();

        for (unsigned char uc : chunk) {
            if (isAlnumAscii(uc)) {
                if (uc >= 'A' && uc <= 'Z') current.push_back(lowerAscii(static_cast<char>(uc)));
                else current.push_back(static_cast<char>(uc));
            } else {
                if (!current.empty()) {
                    out.push_back(current);
                    current.clear();
                }
            }
        }

        leftover = std::move(current);
        return out;
    }

    // Flush any leftover token after EOF
    void finalizeToMap(unordered_map<string,long long> &freq) {
        if (!leftover.empty()) {
            ++freq[leftover];
            leftover.clear();
        }
    }

    vector<string> finalizeToVector() {
        vector<string> v;
        if (!leftover.empty()) {
            v.push_back(leftover);
            leftover.clear();
        }
        return v;
    }
};

// -------------------------------
// VersionedIndexer
// - Builds/stores word -> frequency mapping for a single version
// - Demonstrates function overloading: getFrequency(string) and getFrequency(const char*)
// -------------------------------
class VersionedIndexer {
private:
    string versionName;
    unordered_map<string,long long> freq;

public:
    explicit VersionedIndexer(const string &vname = "") : versionName(vname) {}

    const string &getVersionName() const { return versionName; }

    // Build full index from a file using the provided buffer size
    void buildIndex(const string &filepath, size_t bufferKB) {
        freq.clear();
        // reserve to reduce rehash overhead (tunable)
        freq.reserve(1 << 20);
        BufferedReader reader(filepath, bufferKB);
        Tokenizer tokenizer;
        vector<char> chunk;

        while (reader.readChunk(chunk) > 0) {
            tokenizer.tokenizeToMap(chunk, freq);
        }
        tokenizer.finalizeToMap(freq);
    }

    // Overloaded frequency accessors
    long long getFrequency(const string &word) const {
        auto it = freq.find(word);
        return (it == freq.end() ? 0LL : it->second);
    }

    long long getFrequency(const char *word) const {
        if (!word) return 0;
        return getFrequency(string(word));
    }

    // Return top-K words: sorted descending by frequency, then lexicographically ascending
    vector<pair<string,long long>> getTopK(int k) const {
        vector<pair<string,long long>> items;
        items.reserve(freq.size());
        for (const auto &p : freq) items.emplace_back(p.first, p.second);

        sort(items.begin(), items.end(),
             [](const pair<string,long long> &a, const pair<string,long long> &b) {
                 if (a.second != b.second) return a.second > b.second;
                 return a.first < b.first;
             });

        if ((int)items.size() > k) items.resize(k);
        return items;
    }
};

// -------------------------------
// QueryProcessor (abstract base)
// - demonstrates inheritance + runtime polymorphism
// -------------------------------
class QueryProcessor {
protected:
    size_t bufferKB;
    double execSeconds;

public:
    explicit QueryProcessor(size_t bufKB) : bufferKB(bufKB), execSeconds(0.0) {}
    virtual ~QueryProcessor() = default;

    // Pure virtual: derived classes must implement execution
    virtual void execute() = 0;

    // Common footer printing
    void printFooter() const {
        cout << "Buffer Size (KB): " << bufferKB << "\n";
        cout << "Execution Time (s): " << fixed << setprecision(5) << execSeconds << "\n";
    }
};

// -------------------------------
// SingleVersionQuery (derived)
// - handles "word" and "top" queries for a single file/version
// -------------------------------
class SingleVersionQuery : public QueryProcessor {
private:
    string filePath;
    string versionName;
    string queryType;   // "word" or "top"
    string targetWord;  // for 'word'
    int topK;           // for 'top'

public:
    SingleVersionQuery(size_t bufKB,
                       const string &file,
                       const string &version,
                       const string &qType,
                       const string &word,
                       int k)
        : QueryProcessor(bufKB),
          filePath(file),
          versionName(version),
          queryType(qType),
          targetWord(word),
          topK(k)
    {}

    void execute() override {
        // time the whole indexing + answering for this version
        auto start = chrono::high_resolution_clock::now();

        VersionedIndexer idx(versionName);
        idx.buildIndex(filePath, bufferKB);

        auto stop = chrono::high_resolution_clock::now();
        execSeconds = chrono::duration<double>(stop - start).count();

        if (queryType == "word") {
            cout << "Version: " << versionName << "\n";
            // normalize user target to match tokenization: alphanumeric + lowercase letters
            string normalized;
            normalized.reserve(targetWord.size());
            for (char c : targetWord) {
                unsigned char uc = static_cast<unsigned char>(c);
                if ((uc >= '0' && uc <= '9') || (uc >= 'A' && uc <= 'Z') || (uc >= 'a' && uc <= 'z')) {
                    if (uc >= 'A' && uc <= 'Z') normalized.push_back(char(uc + 32));
                    else normalized.push_back(c);
                }
            }
            cout << "Count: " << idx.getFrequency(normalized) << "\n";
            printFooter();
        }
        else if (queryType == "top") {
            cout << "Top-" << topK << " words in version " << versionName << ":\n";
            auto v = idx.getTopK(topK);
            for (const auto &p : v) cout << p.first << " " << p.second << "\n";
            printFooter();
        }
        else {
            throw AnalyzerException("Unsupported single-version query type: " + queryType);
        }
    }
};

// -------------------------------
// DiffQuery (derived)
// - handles difference query across two versions
// -------------------------------
class DiffQuery : public QueryProcessor {
private:
    string file1, ver1;
    string file2, ver2;
    string targetWord;

public:
    DiffQuery(size_t bufKB,
              const string &f1, const string &v1,
              const string &f2, const string &v2,
              const string &word)
        : QueryProcessor(bufKB), file1(f1), ver1(v1), file2(f2), ver2(v2), targetWord(word) {}

    void execute() override {
        // build both indexes (requirement: maintain separate index per version)
        auto start = chrono::high_resolution_clock::now();

        VersionedIndexer idx1(ver1);
        VersionedIndexer idx2(ver2);
        idx1.buildIndex(file1, bufferKB);
        idx2.buildIndex(file2, bufferKB);

        auto stop = chrono::high_resolution_clock::now();
        execSeconds = chrono::duration<double>(stop - start).count();

        // normalize target
        string normalized;
        normalized.reserve(targetWord.size());
        for (char c : targetWord) {
            unsigned char uc = static_cast<unsigned char>(c);
            if ((uc >= '0' && uc <= '9') || (uc >= 'A' && uc <= 'Z') || (uc >= 'a' && uc <= 'z')) {
                if (uc >= 'A' && uc <= 'Z') normalized.push_back(char(uc + 32));
                else normalized.push_back(c);
            }
        }

        long long f1 = idx1.getFrequency(normalized);
        long long f2 = idx2.getFrequency(normalized);
        long long diff = f2 - f1;

        cout << "Difference (" << ver2 << " - " << ver1 << "): " << diff << "\n";
        printFooter();
    }
};

// -------------------------------
// Simple argument parser (flag -> value)
// Expects flags like --file <path> (pairs). Throws on missing mandatory options.
// -------------------------------
unordered_map<string,string> parseArgs(int argc, char *argv[]) {
    unordered_map<string,string> mp;
    for (int i = 1; i < argc; ++i) {
        string key = argv[i];
        if (key.rfind("--", 0) == 0) {
            if (i + 1 < argc) {
                mp[key] = argv[i+1];
                ++i;
            } else {
                mp[key] = ""; // present without value
            }
        } else {
            // ignore stray tokens
        }
    }
    return mp;
}

// -------------------------------
// main()
// -------------------------------
int main(int argc, char *argv[]) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    try {
        if (argc <= 1) throw AnalyzerException("No arguments provided. See usage in assignment spec.");

        auto args = parseArgs(argc, argv);
        if (!args.count("--query")) throw AnalyzerException("Missing --query argument");

        string query = args["--query"];

        // Buffer default 512 KB if not provided
        size_t bufferKB = 512;
        if (args.count("--buffer") && !args["--buffer"].empty()) {
            bufferKB = (size_t)stoul(args["--buffer"]);
        }
        // Validate buffer (uses template function)
        validateRange<size_t>(bufferKB, 256, 1024, "Buffer must be between 256 and 1024 KB");

        QueryProcessor *proc = nullptr;

        if (query == "word") {
            // needs --file, --version, --word
            if (!args.count("--file")) throw AnalyzerException("Missing --file for word query");
            if (!args.count("--version")) throw AnalyzerException("Missing --version for word query");
            if (!args.count("--word")) throw AnalyzerException("Missing --word for word query");

            proc = new SingleVersionQuery(bufferKB,
                                          args["--file"],
                                          args["--version"],
                                          "word",
                                          args["--word"],
                                          0); // topK unused
        }
        else if (query == "top") {
            if (!args.count("--file")) throw AnalyzerException("Missing --file for top query");
            if (!args.count("--version")) throw AnalyzerException("Missing --version for top query");
            if (!args.count("--top")) throw AnalyzerException("Missing --top for top query");

            int k = stoi(args["--top"]);
            if (k <= 0) throw AnalyzerException("--top must be positive");

            proc = new SingleVersionQuery(bufferKB,
                                          args["--file"],
                                          args["--version"],
                                          "top",
                                          "",
                                          k);
        }
        else if (query == "diff") {
            // needs --file1, --version1, --file2, --version2, --word
            if (!args.count("--file1")) throw AnalyzerException("Missing --file1 for diff query");
            if (!args.count("--version1")) throw AnalyzerException("Missing --version1 for diff query");
            if (!args.count("--file2")) throw AnalyzerException("Missing --file2 for diff query");
            if (!args.count("--version2")) throw AnalyzerException("Missing --version2 for diff query");
            if (!args.count("--word")) throw AnalyzerException("Missing --word for diff query");

            proc = new DiffQuery(bufferKB,
                                 args["--file1"], args["--version1"],
                                 args["--file2"], args["--version2"],
                                 args["--word"]);
        }
        else {
            throw AnalyzerException("Unknown --query value (must be word|top|diff)");
        }

        // dynamic dispatch
        proc->execute();

        delete proc;
    }
    catch (const AnalyzerException &ex) {
        cerr << "Error: " << ex.what() << "\n";
        return 1;
    }
    catch (const exception &ex) {
        cerr << "Fatal: " << ex.what() << "\n";
        return 1;
    }

    return 0;
}