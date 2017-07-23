SRC = main.cpp \
	Image.cpp \
	Scene.cpp \
	Camera.cpp\
	Sphere.cpp\
	Triangle.cpp

OBJ = $(SRC:.cpp=.o)
CFLAGS = -g -std=c++11 -O3

.cpp.o:
	g++ -c $< $(CFLAGS)

all: test

aoa_jpeg.o: jpeg_reader/aoa_jpeg.c
		gcc -c $< -O3

test: $(OBJ) aoa_jpeg.o
		g++ $(OBJ) aoa_jpeg.o -o raytracer $(CPPFLAGS) -ljpeg

clean:
	rm -f *.o raytracer *.ppm
	

