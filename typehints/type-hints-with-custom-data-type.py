import apache_beam as beam
import typing


class Employee(object):
    def __init__(self, id, name):
        self.id = id
        self.name = name


class EmployeeCoder(beam.coders.Coder):

    def encode(self, employee):
        return ('%s:%s' % (employee.id, employee.name)).encode('utf-8')

    def decode(self, s):
        return Employee(*s.decode('utf-8').split(':'))

    def is_deterministic(self):
        return True


beam.coders.registry.register_coder(Employee, EmployeeCoder)


def split_file(input):
    name, id, salary = input.split(',')
    return Employee(id, name), int(salary)


result = (
    p
	| beam.io.ReadFromText('data.txt')
    | beam.Map(split_file)
    | beam.CombinePerKey(sum).with_input_types(typing.Tuple[Employee, int])
	)
	
p.run()	