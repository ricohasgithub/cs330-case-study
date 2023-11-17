import multiprocessing

def main():
    # Start the multiprocessing task
    my_class_instance = MyClass("Sample Data")
    my_class_instance.start_multiprocessing_task()

class MyClass:
    def __init__(self, data):
        self.data = data

    def start_multiprocessing_task(self):
        # Start the multiprocessing task
        processes = []
        for i in range(5):
            process = multiprocessing.Process(target=self.task_function, args=(self.data, i))
            processes.append(process)
            process.start()
        for process in processes:
            process.join()

    @staticmethod
    def task_function(data, i):
        # Perform some task
        print(f"Processing data: {data} {i}")

# Example usage
if __name__ == "__main__":
    main()