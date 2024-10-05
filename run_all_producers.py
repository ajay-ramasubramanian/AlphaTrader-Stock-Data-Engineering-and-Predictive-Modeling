import os
import subprocess
import sys
def run_python_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                print(f"Running {file_path}")
                subprocess.run([sys.executable, file_path])
                # subprocess.run(["python", file_path])

# Specify the directory containing your Python scripts
script_directory = "producers/"
run_python_files(script_directory)