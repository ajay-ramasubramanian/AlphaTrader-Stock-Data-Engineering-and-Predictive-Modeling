# import os
# import subprocess

# import sys

# def run_python_files(directory):
    
#     for root, dirs, files in os.walk(directory):
#         for file in files:
#             if file.endswith(".py"):
#                 file_path = os.path.join(root, file)
#                 print(f"Running {file_path}")
#                 subprocess.run([sys.executable, file_path])
#                 # subprocess.run(["python", file_path])

# # Specify the directory containing your Python scripts
# script_directory = "consumers/"
# run_python_files(script_directory)

import os
import subprocess
import concurrent.futures
import sys

def run_script(file_path):
    print(f"Running {file_path}")
    result = subprocess.run([sys.executable, file_path], capture_output=True, text=True)
    return file_path, result.returncode, result.stdout, result.stderr

def run_python_files_parallel(directory, max_workers=None):
    python_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                python_files.append(os.path.join(root, file))
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(run_script, file_path) for file_path in python_files]
        
        for future in concurrent.futures.as_completed(futures):
            file_path, return_code, stdout, stderr = future.result()
            print(f"Finished {file_path} with return code {return_code}")
            if stdout:
                print(f"stdout:\n{stdout}")
            if stderr:
                print(f"stderr:\n{stderr}")

if __name__ == '__main__':
    script_directory = r"consumers/"  # Replace with your actual directory path
    run_python_files_parallel(script_directory)