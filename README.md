 * This is a simple project to catch other system's event
` 1. support mement event like program exception or error
  2. support long time event
  3. only support send email to notify the happened events
  4. Client buries point in its program via logging

 * deps:
  1. mysql

 * configures
  1. strategy can be set in config.py
   * long time event can set duration and times
   * monment only can set times

 * agent module:
  1. Collect all the client's event and record into mysql database 
 * event_parser module:
  1. Parse all event and send mail if achieving the configure threshold
 
