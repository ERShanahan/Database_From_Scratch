
# Compiler and Flags
CC 	:= gcc
CFLAGS 	:= -Wall -Wextra -Werror -std=c11 -g
LDFLAGS :=

SRCS	:= $(wildcard *.c)
OBJS	:= $(SRCS:.c=.o)

TARGET	:= db

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(LDFLAGS) -c $< -o $@

run: all
	./$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)

