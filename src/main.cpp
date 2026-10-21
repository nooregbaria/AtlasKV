#include "parser.h"
#include <iostream>
#include "wal.h"
#include "Database.h"
#include "command_handler.h"



int main() {


    Database db;
    WAL wal("log.txt");
    std::string command;
    if (!wal.load_wal(db)) {
        std::cerr << "Error loading WAL log" << std::endl;
        return 1;
    }

    while (std::getline(std::cin, command)) {
        std::cout << do_action(command, db, wal).response << std::endl;
    }
    return 0;
}
