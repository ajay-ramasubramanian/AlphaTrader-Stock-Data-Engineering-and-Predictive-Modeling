import subprocess
import os
import sys

def run_script(script_name):
    subprocess.run([sys.executable, script_name], check=True)

if __name__ == '__main__':
    directory = "ingestion/"
    scripts_to_run = []

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                scripts_to_run.append(file_path)
    
    print(f"Running {len(scripts_to_run)} scripts...")
    for script in scripts_to_run:
        print(f"Running {script}...")
        run_script(script)
    print("All scripts have completed execution.")