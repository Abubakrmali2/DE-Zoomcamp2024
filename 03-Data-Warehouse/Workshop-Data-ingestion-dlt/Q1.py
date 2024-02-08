def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 5
generator = square_root_generator(limit)
s=0
for sqrt_value in generator:
     s=s+sqrt_value
     print(sqrt_value)

print (f"the sum of outputs is:{s}")