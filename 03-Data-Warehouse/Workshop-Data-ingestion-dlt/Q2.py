def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 13
generator = square_root_generator(limit)
l=1
for sqrt_value in generator:
     if l==limit:
      print(f'The 13th number yielded by the generator: {sqrt_value}')
     l += 1

