import apache_beam as beam

p2 = beam.Pipeline()

lines = (
            p2
            | beam.Create([
               'Using create transform ',
               'to generate in memory data ',
               'This is 3rd line ',
               'Thanks '])
     
            | beam.io.WriteToText('data/outCreate1')
          )
p2.run()


p3 = beam.Pipeline()

lines1 = (p3
           
           | beam.Create([1,2,3,4,5,6,7,8,9])
           
           | beam.io.WriteToText('data/outCreate2')
          )
p3.run()


p4 = beam.Pipeline()


lines = (p4
           | beam.Create([("maths",52),("english",75),("science",82), ("computer",65),("maths",85)])
         
            | beam.io.WriteToText('data/outCreate3')
          )
p4.run()


p5 = beam.Pipeline()

lines = ( p5
         
       | beam.Create({'row1':[1,2,3,4,5],
                     'row2':[1,2,3,4,5]})
       | beam.Map(lambda element: element)
       | beam.io.WriteToText('data/outCreate4')
  )
  
p5.run()



