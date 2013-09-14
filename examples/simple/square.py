from wok.task import task

@task.foreach
def square(x):

	square = x * x
	task.logger.info("x = {0}, x^2 = {1}".format(x, square))

	x_square_port = task.ports("x_square")
	x_square_port.send(square)

task.run()
