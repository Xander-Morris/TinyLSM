import os
log_file_name = os.getenv("LOG_FILE_NAME", "log_file.txt")

def main():
    with open(log_file_name, 'r') as file:
        content = file.read()
        
        for line in content: 
            print(line)

if __name__ == "__main__":
    main()