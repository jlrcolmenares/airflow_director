FILE_PATH="/tmp/my_temp_file.txt"
if [ -f "$FILE_PATH" ]; then
    echo "File $FILE_PATH exists. Deleting..."
    rm "$FILE_PATH"
else
    echo "File $FILE_PATH does not exist. Creating..."
    touch "$FILE_PATH"
fi