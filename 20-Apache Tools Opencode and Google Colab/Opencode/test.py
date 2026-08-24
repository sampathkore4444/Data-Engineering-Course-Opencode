import numpy as np
import time

N = 10_000_000
income = np.random.randint(0, 1000000, N, dtype=np.int64)

# METHOD 1: Python loop (don't actually run this!)
start = time.time()
tax = np.empty(N, dtype=np.int64)
for i in range(N):
    tax[i] = income[i] * 3 // 10 if income[i] > 50000 else income[i] * 15 // 100
print(f"Python loop: {time.time()-start:.2f}s")  # ~3-5 seconds

# METHOD 2: np.where (C loop + SIMD blend)
start = time.time()
tax = np.where(income > 50000, income * 3 // 10, income * 15 // 100)
print(f"np.where: {time.time()-start:.2f}s")  # ~0.05 seconds (100x faster)

# METHOD 3: Manual boolean indexing (also C loop)
start = time.time()
tax = np.empty(N, dtype=np.int64)
mask = income > 50000
tax[mask] = income[mask] * 3 // 10  # C loop for high
tax[~mask] = income[~mask] * 15 // 100  # C loop for low
print(f"Boolean indexing: {time.time()-start:.2f}s")  # ~0.06 seconds
