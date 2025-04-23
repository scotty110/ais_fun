set -eu

# Check if .tracks environment is activated
if [[ "$VIRTUAL_ENV" != *".tracks"* ]]; then
  echo "Error: The '.tracks' environment is not activated."
  echo "Please run the following command first:"
  echo "  source .tracks/bin/activate"
  exit 1
fi

# Check if the destination directory is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <destination_directory>"
  exit 1
fi

# Set destination directory and create a temporary directory
DEST_DIR=$1
TMP_DIR=$(mktemp -d -p "$DEST_DIR")

# Ensure the temporary directory is created
if [ ! -d "$TMP_DIR" ]; then
  echo "Failed to create temporary directory in $DEST_DIR"
  exit 1
fi

echo "Temporary directory created at $TMP_DIR"

# Arrays for URLs and their corresponding years
BASE_URLS=(
  "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/index.html"
  "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/index.html"
  "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2022/index.html"
  "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2021/index.html"
  "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2020/index.html"
)

YEARS=(
  "2024"
  "2023"
  "2022"
  "2021"
  "2020"
)

# Process each URL and download the index files individually
echo > "$TMP_DIR/urls.txt"
for i in "${!BASE_URLS[@]}"; do
  URL="${BASE_URLS[$i]}"
  YEAR="${YEARS[$i]}"
  echo "Downloading index for year $YEAR..."
  
  # Use curl to download the index page - aria2c has issues with content length mismatches
  curl -s -o "$TMP_DIR/index_${YEAR}.html" "$URL"
  #echo "Would have run curl"
  
  INDEX_FILE="$TMP_DIR/index_${YEAR}.html"
  BASE_DIR=$(dirname "$URL")
  
  # Check if the file exists and has content
  if [ -s "$INDEX_FILE" ]; then
    echo "Processing index for year $YEAR..."
    # Extract zip file links from the index page
    grep -o 'href="[^"]*\.zip"' "$INDEX_FILE" | sed 's/href="//;s/"$//' | while read -r LINK; do
      if [[ "$LINK" != http* ]]; then
        echo "$BASE_DIR/$LINK" >> "$TMP_DIR/urls.txt"
      else
        echo "$LINK" >> "$TMP_DIR/urls.txt"
      fi
    done
    
    # Clean up the index file
    rm "$INDEX_FILE"
  else
    echo "Warning: Failed to download index for year $YEAR"
  fi
done

# Count the number of URLs found
URL_COUNT=$(wc -l < "$TMP_DIR/urls.txt")
echo "Found $URL_COUNT ZIP files to download"

# Download files in parallel using aria2
if [ "$URL_COUNT" -gt 0 ]; then
  echo "Downloading ZIP files using aria2c..."
  aria2c -i "$TMP_DIR/urls.txt" -d "$TMP_DIR" --max-concurrent-downloads=10 --allow-overwrite=true --check-integrity=false
  #echo "Would have run aria2c"
  
  # Unpack the downloaded files
  echo "Unpacking ZIP files..."
  for i in "$TMP_DIR"/*.zip; do
    if [ -f "$i" ]; then
      unzip "$i" -d "$TMP_DIR"
      rm "$i"
    fi
  done
  
  # Run convert.py with the temporary directory as input and destination directory as output
  echo "Running convert.py..."
  python convert.py "$TMP_DIR" "$DEST_DIR"
else
  echo "No ZIP files found to download"
fi

# Clean up the temporary directory
#rm -rf "$TMP_DIR"

echo "Data downloaded, processed, and saved to $DEST_DIR"