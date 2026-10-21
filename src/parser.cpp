
#include "parser.h"

bool is_space(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

std::string delete_extra_spaces(const std::string& str) {
    std::string result;
    bool in_spaces = false;
    for (char c : str) {
        if (!is_space(c)) {
            result += c;
            in_spaces = false;
        } else if (!in_spaces) {
            result += ' ';
            in_spaces = true;
        }
    }
    return result;
}

std::vector<std::string> parse(const std::string& str) {
    std::vector<std::string> parsed;
    std::string cleaned_str = delete_extra_spaces(str);
    std::string current;
    for (char c : cleaned_str) {
        if (c == ' ') {
            if (!current.empty()) {
                parsed.push_back(current);
                current.clear();
            }
        } else {
            current += c;
        }
    }
    if (!current.empty()) {
        parsed.push_back(current);
    }
    return parsed;
}