# number = int(input("Enter number 1 "))
# number2 = int(input("Enter number2 "))
# number3 = int(input("Enter number3 "))
# print(number + number2 + number3)
# average = (number + number2 + number3) / 3
# print(average)
# print(int(max(number, number2,  number3)))
# print(int(min(number, number2, number3)))


# number = int(input("Enter a number: "))
# if number % 2 == 0:
#     print("Even")
# else:
#     print("Odd")
# if number > 0:
#     print("Positive")
# elif number == 0:
#     print("Zero")
# else:
#     print("Negative")

# number = int(input("Enter number 1 "))
# number2 = int(input("Enter number2 "))
# operation = input("Enter operation: ")
# if operation == "+":
#     print(number + number2)
# elif operation == "-":
#     print(number - number2)
# elif operation == "*":
#     print(number * number2)
# elif operation == "/":
#     if number2 == 0:
#         print("Cannot divide by zero")
#     else:
#         print(number / number2)
# else:
#     print("Operation not supported")

# slovo = input("enter some leters ")
# print(len(slovo))
# print(slovo.upper())
# print(slovo.lower())
# print(slovo.startswith("A"))
# print(slovo[::-1])

# spisok = [1,2,5,4,3]
# print(spisok)
# print(len(spisok))
# print(sum(spisok))
# print(max(spisok))
# print(min(spisok))
# spisok.sort()
# print(spisok)
# spisok.append(int(input("Add number ")))
# print(spisok)

# numbers = [2, 5, 8, 11, 14, 17, 20]
# for number in numbers:
#     print(number)
# for number in numbers:
#     if number % 2 == 0:
#         print(number)
# for number in numbers:
#     if number % 2 == 1:
#         print(number)
# total = 0
# for number in numbers:
#     total += number
# print(total)
# total2 = 0
# for number in numbers:
#     if number % 2 == 0:
#         total2 += number
# print(total2)
# total3 = 0
# for number in numbers:
#     if number % 2 == 1:
#         total3 += number
# print(total3)

# numbers1 = []
# numbers = [12, 5, 8, 21, 4, 17, 9, 30, 6]
# for number in numbers:
#     if number > 10:
#         numbers1.append(number)
# print(numbers1)
# print(len(numbers1))
# print(sum(numbers1))
# for num in numbers:
#     if num == 17:
#         print("17 in numbers")
# for num in numbers:
#     if num % 3 == 0:
#         print(num)

# person = {
#     "name": "Alex",
#     "age": 25,
#     "city": "Warsaw",
#     "job": "Developer"
# }
# print(person["name"])
# print(person["age"])
# person["city"] = "Krakow"
# person["salary"] = 5000
# print(person)
# person.pop("job")
# print(person)
# print("salary" in person)

# grades = {
#     "Alex": 85,
#     "Maria": 92,
#     "John": 67,
#     "Anna": 95,
#     "David": 73
# }
#
# for grade in grades.keys():
#     print(grade)
# for grade, value in grades.items():
#     if value > 80:
#         print(grade,value)
# average = sum(grades.values()) / len(grades)
# print(average)
# max_average = max(grades.values())
# for grade, value in grades.items():
#     if value == max_average:
#         print(grade,value)
#

# students = [
#     {"name": "Alex", "age": 22, "grade": 85},
#     {"name": "Maria", "age": 24, "grade": 92},
#     {"name": "John", "age": 21, "grade": 67},
#     {"name": "Anna", "age": 23, "grade": 95},
#     {"name": "David", "age": 25, "grade": 73}
# ]

# for student in students:
#     print(student['name'])
# for student in students:
#     if student['grade'] > 80:
#         print(student['name'], student['grade'])
# total = 0
# for student in students:
#     total += student['grade']
# total = total / len(students)
# print(total)
# max_grade = 0
# for student in students:
#     if student['grade'] > max_grade:
#         max_grade = student['grade']
# for student in students:
#     if student['grade'] == max_grade:
#         print(student['name'], student['grade'])

students = [
    {"name": "Alex", "age": 22, "grade": 85},
    {"name": "Maria", "age": 24, "grade": 92},
    {"name": "John", "age": 21, "grade": 67},
    {"name": "Anna", "age": 23, "grade": 95},
    {"name": "David", "age": 25, "grade": 73},
    {"name": "Kate", "age": 20, "grade": 88}
]

good_students = []
for student in students:
    if student["grade"] > 80:
        good_students.append(student)
print(good_students)
print(len(good_students))
stud = 0
for student in good_students:
    stud += student["grade"]
print(stud)
average = stud / len(good_students)
print(average)
