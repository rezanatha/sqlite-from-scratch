#include <cstring>
#include <iostream>
#include <fstream>

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }

    std::string database_file_path = argv[1];
    std::string command = argv[2];

    if (command == ".dbinfo") {
        std::ifstream database_file;
        database_file.open(database_file_path, std::ios::binary);

        if (!database_file) {
            std::cerr << "Failed to open the database file" << std::endl;
            return 1;
        }

        database_file.seekg(16);  // Skip the first 16 bytes of the header
        
        char buffer[2];
        database_file.read(buffer, 2);
        
        unsigned short page_size = (static_cast<unsigned char>(buffer[1]) | (static_cast<unsigned char>(buffer[0]) << 8));
        
        std::cout << "database page size: " << page_size << std::endl;
    }

    return 0;
}
