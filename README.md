# dirbatcher
Python script for automated mapping, validation, and chunking of ID-named folders — with support for listing, copying, and moving batches.

# 🗂️ ID Folder Mapper & Chunker

A Python utility to **map**, **list**, and **chunk** subfolders named by IDs from a main directory.

This tool is designed for workflows where you have a large number of folders (e.g., applicant IDs, project IDs, dataset IDs) and need to:
- Verify which IDs exist or are missing  
- List the files inside each folder  
- Split matched folders into smaller **chunks (batches)**  
- Optionally **copy or move** chunks to another directory  

---

## 🚀 Features

✅ Map IDs → Folders (detect missing ones)  
✅ List files inside each folder (recursive or top-level)  
✅ Chunk folders into batches for easier processing  
✅ Save chunk lists as `.json` or `.txt` files  
✅ Copy / Move chunks into destination folders  
✅ Works with `.txt` or `.csv` ID lists  

---

## 🧩 Folder Example


IDs file (`ids.txt`):

---

## 🖥️ Installation

Clone this repository and install Python (3.8+ recommended).

```bash
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>

⚙️ Usage

Basic syntax:

python list_and_chunk_id_folders.py --source <main-folder> --ids <ids-file> [options]

✅ 1. List and Map Folders
python list_and_chunk_id_folders.py -s "C:\Users\Haritima\Main" -i "C:\Users\Haritima\ids.txt"


Output:

Total IDs provided: 100
Found folders: 90
Missing folders: 10
Sample mapping + file counts:
  4059 -> C:\Users\Haritima\Main\4059 (files: 2)
  4239 -> C:\Users\Haritima\Main\4239 (files: 3)

✅ 2. Create Chunks (Batches)

Split into chunks of 50 folders each:

python list_and_chunk_id_folders.py -s "C:\Users\Haritima\Main" -i "C:\Users\Haritima\ids.txt" --chunk-size 50


Output:

Total chunks: 4 (chunk size: 50)
  Chunk 1: 50 items (IDs: 4059 ... 4899)
  Chunk 2: 50 items (IDs: 4900 ... 5100)

✅ 3. Save Chunks to Disk

Save the chunks in JSON format:

python list_and_chunk_id_folders.py -s "./Main" -i ids.txt --chunk-size 50 --save-chunks
