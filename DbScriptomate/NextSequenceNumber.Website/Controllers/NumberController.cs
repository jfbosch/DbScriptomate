using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web;
using System.Web.Http;
using NextSequenceNumber.Service;

namespace NextSequenceNumber.Website.Controllers
{
    //[Authorize]
    public class NumberController : ApiController
    {
       /* // GET api/number
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }*/

		// GET api/number/mykey
        public string Get(string key,string password)
        {
	        if (password != System.Configuration.ConfigurationManager.AppSettings.Get("Password"))
	        {
		        return "Invalid password. Please try again";
	        }
	        return NumberStore.GetNextSequenceNumber(key);
        }

		/*// POST api/number
        public void Post([FromBody]string value)
        {
        }

		// PUT api/number/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/values/5
        public void Delete(int id)
        {
        }*/
    }
}
