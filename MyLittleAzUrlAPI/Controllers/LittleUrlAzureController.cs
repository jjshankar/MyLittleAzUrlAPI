using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyLittleAzUrlAPI.Models;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace MyLittleAzUrlAPI.Controllers
{
    [Route("api/littleurl")]
    public class LittleUrlAzureController : Controller
    {
        private LittleUrlAzureContext _azureContext;
        private int _nextUrlId;


        public LittleUrlAzureController()
        {
            _azureContext = new LittleUrlAzureContext();
            _nextUrlId = -1;
        }

        // API Methods
        // Route: api/littleurl
        [HttpGet]
        public IEnumerable<LittleUrlAzure> GetUrls()
        {
            List<LittleUrlAzure> littleUrls = _azureContext.ListUrl().Result;
            return littleUrls;
        }

        // Route: api/littleurl/<key>
        [HttpGet("{key}", Name = "GetByKey")]
        public IActionResult GetByKey(string key)
        {
            if (key.Length == 0)
                return BadRequest("Key value required.");

            LittleUrlAzure item = (LittleUrlAzure)_azureContext.GetUrl(key.ToLower()).Result.Result;
            if (item == null)
                return NotFound("URL does not exist.");

            return Ok(item);
        }

        // Route: api/littleurl
        [HttpPost]
        public IActionResult AddToList([FromBody] LittleUrlAzure lUrl)
        {
            if (lUrl.LongUrl.Length == 0)
                return BadRequest("URL value is required.");

            // Check if the URL already exists
            LittleUrlAzure item = _azureContext.CheckDupe(lUrl.LongUrl).Result;
            if(item == null)
            {
                // create
                item = (LittleUrlAzure)_azureContext.InsertUrl(GetNextId(), lUrl.LongUrl).Result.Result;
            }

            // return created/found item
            if (item != null)
                return CreatedAtRoute("GetByKey", new { key = item.ShortUrl }, item);
            
            return NotFound();
        }

        // Route: api/littleurl
        [HttpDelete("{key}")]
        public IActionResult Delete(string key)
        {
            if (key.Length == 0)
                return BadRequest("URL value is required.");

            // Check if the URL exists
            LittleUrlAzure item = (LittleUrlAzure)_azureContext.GetUrl(key.ToLower(), false).Result.Result;
            if (item == null)
                return NotFound("URL does not exist.");

            // Found document; Delete
            item = (LittleUrlAzure)_azureContext.ToggleDelete(key.ToLower(), true).Result;

            // return deleted item
            return Ok(item);
        }

        // Route: api/littleurl
        [HttpPost("{key}")]
        public IActionResult UnDelete(string key)
        {
            if (key.Length == 0)
                return BadRequest("URL value is required.");

            // Check if the deleted URL exists
            LittleUrlAzure item = (LittleUrlAzure)_azureContext.GetUrl(key.ToLower(), true).Result.Result;
            if (item == null)
                return NotFound("URL does not exist.");

            // Found document; UnDelete
            item = (LittleUrlAzure)_azureContext.ToggleDelete(key.ToLower(), false).Result;

            // return deleted item
            return Ok(item);
        }

        // Private
        private int GetNextId()
        {
            if (_nextUrlId < 0)
                _nextUrlId = _azureContext.ListUrl().Result.Count;

            return ++_nextUrlId;
        }
    }
}
