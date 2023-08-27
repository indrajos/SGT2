# Parallel Matrix Multiplication:
#
# Implement parallel matrix multiplication using the concurrent.futures module to speed up the computation
# of large matrices. (you can declare the matrixes using, ex. lists or tuples)

import concurrent.futures


def matrix_multiply_row(row_a, matrix_b):
    result_row = []
    for col in range(len(matrix_b[0])):
        result = sum(row_a[i] * matrix_b[i][col] for i in range(len(row_a)))
        result_row.append(result)
    return result_row


def parallel_matrix_multiply(matrix_a, matrix_b, num_threads):
    if len(matrix_a[0]) != len(matrix_b):
        raise ValueError("Number of columns in matrix A must be equal to number of rows in matrix B")

    result_matrix = [[0] * len(matrix_b[0]) for _ in range(len(matrix_a))]

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(len(matrix_a)):
            futures.append(executor.submit(matrix_multiply_row, matrix_a[i], matrix_b))

        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            result_matrix[i] = future.result()

    return result_matrix


if __name__ == "__main__":
    matrix_a = [
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9]
    ]

    matrix_b = [
        [9, 8, 7],
        [6, 5, 4],
        [3, 2, 1]
    ]

    num_threads = 2

    result = parallel_matrix_multiply(matrix_a, matrix_b, num_threads)
    for row in result:
        print(row)


