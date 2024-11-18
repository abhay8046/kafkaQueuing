# benchmark.py
import requests
import time
import json
from datetime import datetime
import sys

class Benchmarker:
   def __init__(self, base_url='http://localhost:5001'):
       self.base_url = base_url
       self.start_time = None
       self.end_time = None

   def generate_test_data(self, num_pairs):
       """Generate pairs of numbers for testing"""
       return [f"{i},{i*2}" for i in range(1, num_pairs + 1)]

   def send_batch(self, batch_data, batch_number):
       """Send a batch of data to the server"""
       try:
           response = requests.post(
               f'{self.base_url}/trigger',
               json={
                   'dagId': 'user_input_2sum',
                   'inputs': batch_data
               },
               headers={'Content-Type': 'application/json'}
           )
           print(f"Batch {batch_number} sent - Status: {response.status_code}")
           return response.json()
       except Exception as e:
           print(f"Error sending batch {batch_number}: {str(e)}")
           return None

   def get_metrics(self):
       """Get current metrics from server"""
       try:
           response = requests.get(f'{self.base_url}/metrics')
           return response.json()
       except Exception as e:
           print(f"Error getting metrics: {str(e)}")
           return None

   def save_results(self, results, total_pairs, batch_size):
       """Save benchmark results to file"""
       timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
       filename = f"benchmark_results_{timestamp}.json"
       
       data = {
           "benchmark_config": {
               "total_pairs": total_pairs,
               "batch_size": batch_size,
               "start_time": self.start_time.isoformat() if self.start_time else None,
               "end_time": self.end_time.isoformat() if self.end_time else None,
               "duration_seconds": (self.end_time - self.start_time).total_seconds() if self.start_time and self.end_time else None
           },
           "server_metrics": results
       }
       
       with open(filename, 'w') as f:
           json.dump(data, f, indent=2)
       print(f"\nResults saved to {filename}")

   def run_benchmark(self, total_pairs=1000, batch_size=50):
       """Run the complete benchmark"""
       print(f"\nStarting benchmark with {total_pairs} pairs (batch size: {batch_size})")
       self.start_time = datetime.now()
       
       # Generate test data
       all_data = self.generate_test_data(total_pairs)
       batches = [all_data[i:i+batch_size] for i in range(0, len(all_data), batch_size)]
       
       print(f"\nSending {len(batches)} batches...")
       
       # Send all batches
       for i, batch in enumerate(batches, 1):
           self.send_batch(batch, i)
           print(f"Progress: {i}/{len(batches)} batches sent", end='\r')
           time.sleep(0.1)  # Small delay to prevent overwhelming the server
       
       self.end_time = datetime.now()
       duration = (self.end_time - self.start_time).total_seconds()
       
       print("\n\nWaiting for processing to complete...")
       time.sleep(5)  # Wait for final processing
       
       # Get final metrics
       metrics = self.get_metrics()
       if metrics:
           self.save_results(metrics, total_pairs, batch_size)
           
           # Print summary
           print("\nBenchmark Summary:")
           print(f"Total Duration: {duration:.2f} seconds")
           print(f"Pairs Processed: {total_pairs}")
           print(f"Average Rate: {total_pairs/duration:.2f} pairs/second")
           print("\nDetailed metrics available in the saved results file")

def main():
   # Parse command line arguments
   total_pairs = 1000
   batch_size = 50
   
   if len(sys.argv) > 1:
       try:
           total_pairs = int(sys.argv[1])
       except ValueError:
           print("Invalid number of pairs specified")
           return
           
   if len(sys.argv) > 2:
       try:
           batch_size = int(sys.argv[2])
       except ValueError:
           print("Invalid batch size specified")
           return
   
   # Run benchmark
   benchmarker = Benchmarker()
   benchmarker.run_benchmark(total_pairs, batch_size)

if __name__ == "__main__":
   main()