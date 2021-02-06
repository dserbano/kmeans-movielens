import csv, time, sys, math, os

csv.field_size_limit(100000000)

def jaccard_similarity(list1:list, list2:list):
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(list1) + len(list2)) - intersection
    return 1 - float(intersection) / union

def cosine_similarity(v1, v2):
    sumxx, sumxy, sumyy = 0, 0, 0
    if len(v1) == len(v2):
        for i in range(len(v1)):
            x = float(v1[i]); y = float(v2[i])
            sumxx += x*x
            sumyy += y*y
            sumxy += x*y
        return sumxy/math.sqrt(sumxx*sumyy)
    else:
        return 1
    
def average(arr):
    if len(arr) > 0:
        return sum(arr)/len(arr)
    else:
        return 0

    
def calc_avg_std(members:list):
    if len(members) > 1:
        data = [float(i) for i in members]
        return [str(sum(data)/len(data)), str(math.sqrt(((sum(x**2 for x in data) - (sum(data)**2)/len(data))/(len(data) - 1))))]
    else: 
        return None

def findItem(arr, el, key):
    found = None
    i = 0
    while (i < len(arr) and found == None):
        if arr[i][key] == el:
            found = arr[i]
        i = i + 1
    return found


def indexOf(arr:list, el):
    found = -1
    i = 0
    while (i < len(arr) and found == -1):
        if arr[i] == el:
            found = i
        i = i + 1
    return found

class CSVMemoryEfficientOperations:

    @staticmethod
    def readChunk(in_file:str, start_at:int, offsets: list, chunk_size:int, options:list):
        rows = []

        if start_at >= len(offsets):
            return

        csv_file_r = open(in_file, newline='')
        csv_reader = csv.reader(csv_file_r, delimiter=',', quotechar='"')

        csv_file_r.seek(offsets[start_at] if start_at > 0 else 0)

        headings = None
        if start_at == 0:
            headings = next(csv_reader)

        count = 0
        while True:
            line = next(csv_reader, None)
            if line and count < chunk_size:
                for option in options:
                    lambda_f = option["decode"] if option["decode"] else lambda x: x
                    line[option["feature"]] = lambda_f(line[option["feature"]])

                rows.append(line)
                count = count + 1
            else:
                break
        csv_file_r.close()
        return rows
    
    @staticmethod
    def readRow(in_file:str, col:int, key: int, offset:int = None):
        rows = []

        csv_file_r = open(in_file, newline='')
        csv_reader = csv.reader(csv_file_r, delimiter=',', quotechar='"')

        headings = next(csv_reader, None)

        if offset != None:
            csv_file_r.seek(offset)

        line = next(csv_reader, None)
        found = False
        while line and found == False:
            
            if line[col] == key:
                found = True
            line = next(csv_reader, None)
            
        csv_file_r.close()
        return line
    
    @staticmethod
    def readColumn(in_file:str, col:int):
        rows = []

        csv_file_r = open(in_file, newline='')
        csv_reader = csv.reader(csv_file_r, delimiter=',', quotechar='"')

        headings = next(csv_reader, None)

        line = next(csv_reader, None)

        column = []
        while line:
            
            column.append(line[col])
            line = next(csv_reader, None)
            
        csv_file_r.close()
        return column
    
    @staticmethod
    def writeChunk(out_file:str, start_pos:int, chunk: list, headings:list):
        
        csv_file_w = open(out_file, 'w' if start_pos == 0 else 'a', newline='') 
        writer = csv.DictWriter(csv_file_w, fieldnames=headings)
    
        if start_pos == 0:
            writer.writeheader()

        for i in range(0, len(chunk)):

            line = {}
            for j in range(0, len(chunk[i])):
                line[headings[j]] = chunk[i][j]
            writer.writerow(line)

        csv_file_w.close()


    @staticmethod
    def cleanFile(in_file:str, out_file:str):
        """
        description: cleans the file of bad lines and lines encoded improperly (I had some issues due to this);
        @in_file: path of the csv file to clean;
        @out_file: path of the result;
        """
        csv_file_r = open(in_file, newline='')
        csv_reader = csv.reader(csv_file_r, delimiter=',', quotechar='"')
        headings = next(csv_reader)

        csv_file_w = open(out_file, 'w', newline='') 
        csv_writer = csv.writer(csv_file_w, delimiter=',', quotechar='"')
        csv_writer.writerow(headings)

        while True:
            try:
                line = next(csv_reader, None)
                if line == None:
                    break
                if len(line) == len(headings):
                    csv_writer.writerow(line)
            except UnicodeDecodeError:
                continue

        

    @staticmethod
    def getHeadings(in_file:str):
        """
        description: return the headings of a csv;
        @in_file: path of the csv file.
        """
        csv_file_r = open(in_file, newline='')
        csv_reader = csv.reader(csv_file_r, delimiter=',', quotechar='"')
        headings = next(csv_reader)
        csv_file_r.close()
        return headings
        
    
    @staticmethod
    def deleteFile(in_file:str):
        """
        description: delets a file;
        @in_file: path of the csv file to delete.
        """
        os.remove(in_file)
        return

    @staticmethod
    def countFile(in_file:str):
        """
        description: counts a file (no random access);
        @in_file: path of the csv file to count.
        """
        csv_file_r = open(in_file, newline='', errors='ignore')
        csv_reader = csv.reader(csv_file_r, delimiter=',', quotechar='"')
        count = 0

        while True:
            line = next(csv_reader, None)
            if line == None:
                break
            else:
                count = count + 1
        csv_file_r.close()
        del csv_file_r, csv_reader
        return count

    @staticmethod
    def maxColumn(in_file:str, column_id:int):
        """
        description: calculates the maximum value for a column;
        @in_file: path of the csv file;
        @column_id: nr of the column.
        """
        csv_file_r = open(in_file, newline='', errors='ignore')
        csv_reader = csv.reader(csv_file_r, delimiter=',', quotechar='"')
        headings = next(csv_reader, None)
        line = next(csv_reader, None)

        maximum = float("inf")

        while line:
            if (float(line[column_id]) > float(maximum)):
                maximum = line[column_id]
            line = next(csv_reader, None)
           
        csv_file_r.close()
        del csv_file_r, csv_reader, headings
        return maximum

    @staticmethod
    def getLinesOffsets(in_file:str, step:int = 1):
        """
        description: returns an array with the offsets in bytes for every line;
        @in_file: path of the csv file for which to calculate rows offsets;
        @step: step of the offsets.
        """
        csv_file = open(in_file, 'rb')

        offsets = []

        count = 0
        while (True):  
            line = csv_file.readline()
            if line:
                count = count + 1
                if count % step == 0:
                    offsets.append(int(csv_file.tell()))
            else: 
                break

        csv_file.close()
        del csv_file
        return offsets

               
    @staticmethod
    def sortByOffsets(in_file:str, out_file:str, sorted_offsets:list):
        """
        description: sorts the csv with a list of sorted offsets in bytes for every line (in a random access fashion)
        @in_file: path of the input csv file;
        @out_file: path of the output csv file.
        """
        csv_file_r = open(in_file, newline='')
        csv_reader = csv.reader(csv_file_r, delimiter=',', quotechar='"')

        headings = next(csv_reader)
        csv_file_w = open(out_file, 'w', newline='') 
        csv_writer = csv.writer(csv_file_w, delimiter=',', quotechar='"')
        csv_writer.writerow(headings)

        f = open(in_file, newline='')
        reader = csv.reader(f, delimiter=',', quotechar='"')

        count = 0
        for N in sorted_offsets:
            start_iter = time.perf_counter()    
            f.seek(N)
            line = next(reader, None)
            csv_writer.writerow(line)
            stop_iter = time.perf_counter()
            del line, start_iter, stop_iter
            count = count + 1

        f.close()
        csv_file_r.close()
        csv_file_w.close()
        return
        
    @staticmethod
    def sortByKey(in_file:str, out_file:str, key:int):
        """
        @in_file: path of the csv file to sort;
        @out_file: path of the result;
        @key: column nr of the key to sort (the key value has to be int);       
        """
        start_exec = time.perf_counter()    

        offsets = CSVMemoryEfficientOperations.getLinesOffsets(in_file)
        print("size of offsets sortByKey ", in_file, out_file, key, str(sys.getsizeof(offsets)))

        ids = CSVMemoryEfficientOperations.readColumn(in_file, key)

        sorted_indexes = sorted(range(len(ids)), key=ids.__getitem__)
        
        del ids

        print("size of sorted indexes sortByKey ", in_file, out_file, key, str(sys.getsizeof(sorted_indexes)))

        sorted_offsets = [offsets[i] for i in sorted_indexes]
        del offsets, sorted_indexes
        CSVMemoryEfficientOperations.sortByOffsets(in_file, out_file, sorted_offsets)
            
        stop_exec = time.perf_counter()    

        print("EXEC TIME sortByKey", in_file, out_file, key, f"{stop_exec - start_exec:0.4f} seconds")
        return
    
    @staticmethod
    def sortByKeyByGroup(in_file:str, out_file:str, key:int, group_key: int):
        """
        description: sort a csv column based on a key and a group;
        @in_file: path of the csv file to sort;
        @out_file: path of the result;
        @key: column nr of the key to sort (the key value has to be int and pre-sorted);
        @group_key: column nr of group where to sort is contrained to (the key value has to be int).
        """
        start_exec = time.perf_counter()    

        offsets = CSVMemoryEfficientOperations.getLinesOffsets(in_file)
        print("Size of offsets sortByKeyByGroup", in_file, out_file, key, group_key, str(sys.getsizeof(offsets)))

        csv_file_r = open(in_file, newline='')
        csv_reader = csv.reader(csv_file_r, delimiter=',', quotechar='"')

        headings = next(csv_reader)
        line = next(csv_reader)
        
        ids = []
        curr_group = float('inf')
        count = 0

        while (line):
            if float(line[group_key]) != float(curr_group):
                ids.append({"offset": count, "members": [line[key]]})
                curr_group = line[group_key]
            else:
                ids[len(ids) - 1]["members"].append(line[key])
            line = next(csv_reader, None)
            count = count + 1

        sorted_indexes = []
        for i in ids:
            sorted_array = sorted(range(len(i["members"])), key=i["members"].__getitem__)
            for j in sorted_array:
                sorted_indexes.append(j + i["offset"])
            del sorted_array

        csv_file_r.close()                
        del ids, curr_group, count

        print("Size of sorted indexes", in_file, out_file, key, group_key, str(sys.getsizeof(sorted_indexes)))

        sorted_offsets = [offsets[i] for i in sorted_indexes]
        del offsets, sorted_indexes
        CSVMemoryEfficientOperations.sortByOffsets(in_file, out_file, sorted_offsets)
        stop_exec = time.perf_counter()    

        print("EXEC TIME sortByKeyByGroup ", in_file, out_file, key, group_key, f"{stop_exec - start_exec:0.4f} seconds")
        return


    @staticmethod
    def groupColumnsByKey(in_file:str, out_file:str, columns:list, group_key: int, func = None):
        """
        description: group a csv column or many columns based on a key 
        @in_file: path of the csv file to group;
        @out_file: path of the result;
        @key: column nr of the key (the key value has to be int and pre-sorted);
        @group_key: nr of group column.
        """
        start_exec = time.perf_counter()    

        csv_file_r = open(in_file, newline='')
        csv_reader = csv.reader(csv_file_r, delimiter=',', quotechar='"')
        headings = next(csv_reader)

        csv_file_w = open(out_file, 'w', newline='') 
        new_col = '-'.join([headings[col] for col in columns])
        fieldnames = [headings[group_key], new_col]
        writer = csv.DictWriter(csv_file_w, fieldnames=fieldnames)
        writer.writeheader()

        curr_group = float('inf')
        members = []
        
        line = next(csv_reader)

        while (line):
            if float(line[group_key]) != curr_group:
                if curr_group != float('inf'):
                    res = (members if func == None else func(members))

                    if res != None:
                        writer.writerow({fieldnames[0]: str(curr_group), fieldnames[1]: "|".join(res)})
                curr_group = int(line[group_key])
                members = ['-'.join([line[col] for col in columns])]
            else:
                members.append('-'.join([line[col] for col in columns]))
            line = next(csv_reader, None)

        del members
        stop_exec = time.perf_counter()    
        csv_file_r.close()
        csv_file_w.close()
        return
    
    @staticmethod
    def joinByKey(in_files:list, out_file:str, options:list):
        """
        description: joins files based on a common key. The leftmost file of in_file is used as the reference and it must have all the ids in order with no gaps
        @in_files: path of the csv files to join;
        @out_file: path where to save the result;
        @options: options of the files to join;
        """
        files = []
        readers = []
        headings = []

        for i in range(0, len(in_files)):
            files.append(open(in_files[i], newline='', errors='ignore'))
            readers.append(csv.reader(files[i], delimiter=',', quotechar='"'))
            headings.append(next(readers[i]))

        csv_file_w = open(out_file, 'w', newline='') 

        fieldnames = [headings[0][options[0]["key"]]]
        for i in range(0, len(options)):
            for j in range(0, len(options[i]["columns"])):
                fieldnames.append(headings[i][options[i]["columns"][j]])
            
        print(fieldnames)
        writer = csv.DictWriter(csv_file_w, fieldnames=fieldnames)
        writer.writeheader()  

        lines = [None]*len(in_files)
        update = [True]*len(in_files)
        size = CSVMemoryEfficientOperations.countFile(in_files[0]) - 1
        new_line = {}
        curr_key = float('inf')

        for k in range(0, size):
            for f in range(0, len(readers)):
                if update[f] == True:
                    lines[f] = next(readers[f], None)
                if f == 0 and lines[f] != None:
                    curr_key = lines[f][options[f]["key"]]
                    new_line.update({fieldnames[f] : curr_key})
                for j in range(0, len(options[f]["columns"])):
                    val = None
                    if lines[f] != None:
                        val = lines[f][options[f]["columns"][j]]
                        if curr_key < lines[f][options[f]["key"]]:
                            update[f] = False
                            val = None
                        else:
                            update[f] = True
                    
                    new_line.update({headings[f][options[f]["columns"][j]] : val})
            
            writer.writerow(new_line)
            new_line = {}

        return

        10, 201, 50000

    @staticmethod
    def localitySensitiveHashing(line:list, max_cols:int, r = 10, rows_signature = 201, k_buckets = 50000):
        """
        description: given a row having a certain form it computes the bucket keys using locality sensitive hashing
        """
        signature_col = []
        for i in range(1, rows_signature):  
            min_value = min([(int(item[0])*i*70 + 1)%max_cols for item in line[1]])
            signature_col.append(min_value)

        bands = [signature_col[i:i + int(r)] for i in range(0, (r - 1) * int(r), int(r))]
        bucket_keys = []
        for i in range(0, len(bands)):
            bucket_keys.append(int(sum(bands[i])%k_buckets))

        del bands
        return bucket_keys

    @staticmethod
    def mapToLHSBuckets(in_file:str, out_file:str, max_cols:int, r = 10, rows_signature = 201, k_buckets = 50000):
        """
        description: this is able to map a csv file having a certain form to a csv having the bucket as a key and the ids of some elements as value
        """

        csv_file_r = open(in_file, newline='')
        csv_reader = csv.reader(csv_file_r, delimiter=',', quotechar='"')
        headings_reader = next(csv_reader, None)

        headings = ["buckets", "ids"]

        csv_file_w = open(out_file, 'w', newline='') 
        writer = csv.DictWriter(csv_file_w, fieldnames=headings)

        writer.writeheader()

        line = next(csv_reader, None)

        count = 0
        
        while(line):

            line[1] = [item.split("-") for item in line[1].split("|")]

            bucket_keys = CSVMemoryEfficientOperations.localitySensitiveHashing(line, max_cols, r, rows_signature, k_buckets)
            for key in bucket_keys:
                writer.writerow({headings[0]: key, headings[1]: line[0]})

            del bucket_keys
            count = count + 1
            print(count)
            line = next(csv_reader, None)

