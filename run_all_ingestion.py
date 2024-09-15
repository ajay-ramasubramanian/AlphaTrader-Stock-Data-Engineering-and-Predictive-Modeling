import multiprocessing
import subprocess
import os
import sys

class ScriptRunner:
    def __init__(self, scripts):
        self.scripts = scripts
        self.processes = []

    def run_script(self, script_name):
        subprocess.run(['python', script_name])

    def start_processes(self):
        for script in self.scripts:
            process = multiprocessing.Process(target=self.run_script, args=(script,))
            self.processes.append(process)
            process.start()

    def wait_for_completion(self):
        for process in self.processes:
            process.join()

    def run_all(self):
        self.start_processes()
        self.wait_for_completion()
        print("All scripts have completed execution.")

if __name__ == '__main__':
    directory ="ingestion/"
    scripts_to_run =[]

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                if not file in ('retrieve_objects.py', 'utils.py'):
                    file_path = os.path.join(root, file)
                    scripts_to_run.append(file_path)
                    
    runner = ScriptRunner(scripts_to_run)
    runner.run_all()