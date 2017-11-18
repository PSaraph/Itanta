/*
 * +----------------------------------------------------------------------------------------------+
 * The WEB service class
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using NLog;
using System;
using System.Net;
using System.Text;
using System.Threading;

namespace ItantProcessor
{
    public class CItantaFileManagerRqstProcessor : IDisposable
    {
        private readonly HttpListener _listener = new HttpListener();
        private readonly Func<HttpListenerContext, string> _responderMethod;
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();

        public CItantaFileManagerRqstProcessor(string[] prefixes, Func<HttpListenerContext, string> method)
        {
            LOGGER.Info("Intialising the CItantaFileManagerRqstProcessor Server Responder.");
            if (!HttpListener.IsSupported)
            {
                LOGGER.Info("Needs Windows XP SP2, Server 2003 or later.");
                throw new NotSupportedException(
                    "Needs Windows XP SP2, Server 2003 or later.");
            }

            // URI prefixes are required, for example 
            // "http://localhost:8080/index/".
            if (prefixes == null || prefixes.Length == 0)
            {
                LOGGER.Info("Prefixes cannot be Empty");
                throw new ArgumentException("prefixes");
            }

            //A responder method is always required.
            if (method == null)
            {
                LOGGER.Info("A method filemanager/* is required");
                throw new ArgumentException("method");
            }

            foreach (string s in prefixes)
            {
                _listener.Prefixes.Add(s);
            }
            _responderMethod = method;
            LOGGER.Info("Now starting to listen for File Manager Requests");
            _listener.Start();
        }

        public CItantaFileManagerRqstProcessor(Func<HttpListenerContext, string> method, params string[] prefixes)
            : this(prefixes, method) { }

        public void Run()
        {
            ThreadPool.QueueUserWorkItem((o) =>
            {
                LOGGER.Info("Web Server Running...");
                try
                {
                    while (_listener.IsListening)
                    {
                        ThreadPool.QueueUserWorkItem((c) =>
                        {
                            LOGGER.Info("Request Received");
                            var ctx = c as HttpListenerContext;
                            try
                            {

                                string rstr = _responderMethod(ctx);
                                byte[] buf = Encoding.UTF8.GetBytes(rstr);
                                ctx.Response.ContentLength64 = buf.Length;
                                ctx.Response.OutputStream.Write(buf, 0, buf.Length);
                            }
                            catch (Exception ex)
                            {
                                LOGGER.Info(ex.ToString());
                            } // suppress any exceptions
                            finally
                            {
                                // always close the stream
                                ctx.Response.OutputStream.Close();
                            }
                        }, _listener.GetContext());
                    }
                }
                catch { } // suppress any exceptions
            });
        }

        public void Stop()
        {
            LOGGER.Info("Stopping the Server");

            _listener.Stop();
            _listener.Close();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // The bulk of the clean-up code is implemented in Dispose(bool)  
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // free managed resources  
                _listener.Stop();
                _listener.Close();
            }
         }
    }
}
