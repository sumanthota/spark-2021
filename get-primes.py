# https://projecteuler.net/problem=7

# 10001st prime
# Problem 7 
# By listing the first six prime numbers: 2, 3, 5, 7, 11, and 13, we can see that the 6th prime is 13.

# What is the 10 001st prime number?


print("10001st prime")

def get_primes_gt(primes):
	last_prime = primes[len(primes)-1]
	# print(last_prime)
	if last_prime == 2:
		primes.append(3)
		return primes
	else:
		# possible next prime
		pnp = last_prime+1
		while True:
			# print([p for p in primes if p <= pnp//2+1])
			for k in [p for p in primes if p < pnp//2+1]:
				if pnp%k == 0:
					break
			else:
				primes.append(pnp)
				break
			pnp += 1
		return primes

# p = [2]
# print(get_primes_gt(p))

primes = [2]

while True:
	primes = get_primes_gt(primes)
	l = len(primes)
	if l%100 == 0:
		print("reached ", l)
	if l > 10001:
		break

print("10001st prime: ", primes[10000])

# Answer:  104743