import os
log_file_name = os.getenv("LOG_FILE_NAME", "log_file.txt")

def main():
    with open(log_file_name, 'r') as file:
        for line in file: 
            print(line.strip())

if __name__ == "__main__":
    main()