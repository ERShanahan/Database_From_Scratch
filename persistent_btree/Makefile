
# Compiler and Flags
CC 	:= gcc
CFLAGS 	:= -Wall -Wextra -Werror -std=c11 -g -MMD -MP
LDFLAGS :=

SRCS	:= $(wildcard *.c)
OBJS	:= $(SRCS:.c=.o)
DEPS	:= $(SRCS:.c=.d)

TARGET	:= db

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(LDFLAGS) -c $< -o $@

-include $(DEPS)

run: all
	./$(TARGET)

clean:
	rm -f $(OBJS) $(DEPS) $(TARGET)

