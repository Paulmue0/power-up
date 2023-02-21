def fancy_choice(question: str, option1: str, option2: str) -> int:
    """
    Asks the user a question with two options to choose from.
    Returns the user's choice (either 1 or 2).
    """
    print("="*50)
    print(f"{question}\n")
    print(f"  1. {option1}")
    print(f"  2. {option2}")
    print("="*50)

    while True:
        choice = input("Enter your choice (1 or 2): ")
        if choice == "1" or choice == "2":
            break
        print("Invalid choice. Please enter 1 or 2.")

    return int(choice)


def fancy_input_number(prompts: list[str], err_msg: str = "", min_value: int = -1) -> int:
    """
    Asks the user to enter a number.
    Optionally you can parse a minimum value.
    Returns the number as an int.
    """
    print("="*50)
    for prompt in prompts:
        print(prompt)
    print("="*50)

    while True:
        try:
            number = int(input("Enter a number: "))
            if number < min_value or number < 0:
                print(err_msg)
                print(
                    f"Your Value must be greater than or equal to {min_value} and positive.")
            else:
                break
        except ValueError:
            print("Input must be a valid integer.")

    return number
