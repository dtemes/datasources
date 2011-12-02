<?php
/**
 * A CakePhP Datasource for Amazon SDB
 *
 * Original implementation by David Temes Nov 2011
 * 
 * Based on the work from Yasushi Ichikawa http://github.com/ichikaway/
 * and the array_source by the CakePHP team
 *
 * This Datasource uses the amazon SDK for PKP from http://aws.amazon.com/es/sdkforphp/
 * 
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * @copyright Copyright 2005-2010, Cake Software Foundation, Inc. (http://cakefoundation.org)
 * @link http://cakephp.org CakePHP(tm) Project
 * @package datasources
 * @subpackage datasources.models.datasources
 * @since CakePHP Datasources v 0.3
 * @license MIT License (http://www.opensource.org/licenses/mit-license.php)
 *
 *
 * Installation: 
 * 		- Put the AWS sdk files in vendors/aws
 * 
 * Sample config file:
 *
 * var $sdb = array(
 *		'datasource' => 'datasources.sdb',
 *		'AWS_KEY'=> 'YOUR AWS KEY',
 *		'AWS_SECRET_KEY'=>'YOUR SECRET KEY+0eYoANumEPKavBmM',
 *		'AWS_ACCOUNT_ID'=>'YOUR ACCOUNT ID',
 *		'AWS_CANONICAL_ID'=>'YOUR CANONICAL ID',
 *		'AWS_CANONICAL_NAME'=>'David Temes',
 *		//'cache_config'=>'apc',
 *		//'cache_time'=>60,
 *	);
 * 
 * 
 */ 
 
//TODO: Check update when posting multiple attributes with same name
//TODO: belongsto, hasone, hasandbelongstomany
//TODO: use token to get more data in related queries


class sdbSource extends DataSource {
    
/**
 * The sdb instance
 */
protected $_sdb = null;

/**
 * cache configuration
 */
protected $_cache_config=null;

/**
 * Seconds to store data in cache. null=no cache
 */
protected $_cache=null;

/**
 * we use string columns, having this avoids problems with the created,modified and updated fields
 * @var array
 */
public $columns=array('string'=>array());

/**
 * List of requests ("queries")
 *
 * @var array
 */
protected $_requestsLog = array();


/**
 * Constructor, initialize AWS credentials and default settings
 */
public function __construct($config) {
	define('AWS_KEY', $config['AWS_KEY']);
	define('AWS_SECRET_KEY', $config['AWS_SECRET_KEY']);
	define('AWS_ACCOUNT_ID', $config['AWS_ACCOUNT_ID']);
	define('AWS_CANONICAL_ID', $config['AWS_CANONICAL_ID']);
	define('AWS_CANONICAL_NAME', $config['AWS_CANONICAL_NAME']);
	
	App::import('Vendor', 'aws',array('file' =>'aws'.DS.'sdk.class.php'));   
	
	$this->_sdb = new AmazonSDB();
	
	if (isset($config['cache_config'])){
		$this->_sdb->set_cache_config($config['cache_config']);
		$this->_cache_config=$config['cache_config'];
		$this->_cache=60; //default cache for 60 seconds
		}
	if (isset($config['cache_time'])){
		$this->_cache=$config['cache_time'];
		}
	
	if (isset($config['region'])){
		$this->_sdb->set_region($config['region']);
	}
	
	parent::__construct($config);
	}
	
	
/**
* Returns a Model description (metadata) with the very minimum.
*
* @param Model $model
* @return array 
*/
public function describe(&$model) {
	return array('id' => array('type'=>'string',
								'key' => 'primary',
								'length' => 95,
								),
				 'created'=>array('type'=>'string',
								'length' => 10,
								),			
				 'updated'=>array('type'=>'string',
								'length' => 10,
								),
				 'modified'=>array('type'=>'string',
								'length' => 10,
								),				
								
				);
}

/**
* List sources
*
* @param mixed $data
* @return boolean Always false. It's not supported
*/
public function listSources($data = null) {
return false;
}

/**
* Perform a read operation, using getAtributes if we use the primary key or a select if not
*/
public function read(&$model, $queryData = array()) {
	$results=array();
	$i=0;
	$where="";
	$limit="";
	$order="";
	$primary_key=null;
	$found=false;
	$fields=array();
	$conditions=array();
	
	//override cache config with model config
	if (isset ($model->cache_config)){
		$this->_cache_config=$model->cache_config;
		$this->_cache=isset($model->cache_time)?$model->cache_time:'1 minute';
	}
	
	$startTime=getMicrotime();
	//DEBUG($queryData);
	
	if (!empty($queryData['conditions'])) {
		$queryData['conditions'] = (array)$queryData['conditions'];
		
		foreach ($queryData['conditions'] as $key=>$value) {
			if ($value){
				$key=str_replace($model->alias.'.','',$key);
				
				if ($key==$model->primaryKey){
					$primary_key=$value;
					continue;
				}
				
				$conditions[]=$key."='".$value."'";
				//$conditions[]=$join;
			}
		}
		
		/*$conditions=Set::extract('{n}.'.$model->alias, $queryData['conditions']);
		DEBUG($queryData['conditions']);
		if (isset($conditions[$model->primaryKey])){
				$primary_key=$conditions[$model->primaryKey];
				unset($conditions[$model->primaryKey]);
		}*/
		
	}
	
	//fields
	
	if (!empty($queryData['fields'])) {
		$queryData['fields'] = (array)$queryData['fields'];
		foreach ($queryData['fields'] as $key=>$value) {
			if ($value){
				$value=str_replace($model->alias.'.','',$value);
				$fields[]=$value;
			}
		}
	}	
	
	/*if (isset($queryData['fields'])){
		$queryData['fields'] = (array)$queryData['fields'];
		unset($queryData['fields']['count']);
	}*/
	
	
	//DEBUG($fields);
	//limit
	if (!empty($queryData['limit'])) {
		$limit=" limit ".$queryData['limit'];
		}
	
	if (count($fields)==0){
		$fields[]='*';
	}
	
	//order
	if (!empty($queryData['order'])) {
		
		$order=array();
		
		if (is_array($queryData['order'][0])){
			foreach ($queryData['order'][0] as $key=>$value) {
			
				if ($key){
				$key=str_replace($model->alias.'.','',$key);
				
				$order[]=" ".$key." ".$value;
				$order[]=',';
				$conditions[]=$key. ' is not null';
				}
			}
			unset($order[sizeof($order)-1]);
			
		}
		//DEBUG($order);
		if (count($order)>0) {
			$order=" order by ".implode($order,',');
		} else {
			$order="";
		}
	}
	
	//DEBUG($order);
	
	$options=array();
	if (isset($model->ConsistentRead) && $model->ConsistentRead==true){
		$options['ConsistentRead']='true';
		$this->_cache=null;
	}
	
	$fields=implode(',',$fields);
	
	if (count($conditions)>0) {
			$where=" where ".implode($conditions,' and ');
		} else {
			$where="";
		}
	
	// are we using a primary key? if yes get data with getAttributes directly
	if ($primary_key!=null){
		//DEBUG("getAttributes(".$model->useTable.",".$primary_key.",".print_r($queryData['fields'],true).")");
		$fieldspk=$fields;
		if (trim($fieldspk)=='count(*)') $fieldspk="";
		if (trim($fieldspk)=='*') $fieldspk="";
		
		if ($this->_cache){
			$response=$this->_sdb->cache($this->_cache)->getAttributes($model->useTable,$primary_key,$fieldspk,$options);
			} else {
			$response=$this->_sdb->getAttributes($model->useTable,$primary_key,$fieldspk,$options);
			}
			
			if (!$response->isOK()){
				$this->_registerLog($model, "getAttributes(".$model->useTable.",".$primary_key.",".print_r($fieldspk,true).") cache=".$this->_cache, getMicrotime() - $startTime, 0,$response->isOK());
				return null;
			}
			
			$data=array();
		
			
			foreach ($response->body->GetAttributesResult->Attribute as $attribute){
				
				if (isset($data[(string)$attribute->Name])){
					$data[(string)$attribute->Name]=array($data[(string)$attribute->Name]);
					$data[(string)$attribute->Name][]=(string)$attribute->Value;
					}
					else{
					$data[(string)$attribute->Name]=(string)$attribute->Value;
					}
				$found=true;
			}
			if ($found){
				$data['id']=$primary_key;
				$data['itemName']=$primary_key;
			}
			
			//check conditions
			foreach ($queryData['conditions'] as $key=>$value){
				$key=str_replace($model->alias.'.','',$key);
				if (isset($data[$key]) && $data[$key]!=$value){
					$found=false;
					continue;
				}
			}
			
			//DEBUG($found);
			if ($found){
				$data['count']=1;
				$model->id=$primary_key;
				//$data['id']=$primary_key;
				$results[$i++][$model->alias]=$data;
			}
			//DEBUG($results);
			$this->_registerLog($model, "getAttributes(".$model->useTable.",".$primary_key.",".print_r($fieldspk,true).") cache=".$this->_cache, getMicrotime() - $startTime, count($results),true);
			$this->log("getAttributes",'query');
	}
	else{
		//if we dont have a primary key deal with it as a select
		//$fields=isset($queryData['fields'])?implode(',',$queryData['fields']):'*';
		$query='select '.$fields.' from '.$model->useTable.$where.$order.$limit;
		$next_token=null;
		
		//Offset (paging)
		//Get token by specifying records to skip and using count(1) not to retrieve attributes
		if (isset($queryData['offset'])){
			//DEBUG("Offset:".$queryData['offset']);
			$skip=" limit ".$queryData['offset'];
			
			$queryOffset='select count(*) from '.$model->useTable.$where.$order.$skip;
			
			if ($this->_cache){
				$response=$this->_sdb->cache($this->_cache)->select($queryOffset,$options);
			} else {
				$response=$this->_sdb->select($queryOffset,$options);
			}
			if ($response->isOK()){
				$next_token = isset($response->body->SelectResult->NextToken)
				? (string) $response->body->SelectResult->NextToken
				: null;
			}
			$this->_registerLog($model, $queryOffset." cache=".$this->_cache, getMicrotime() - $startTime, 1,$response->isOK());
		}
		
		do {
				if ($next_token){
					$options['NextToken']=$next_token;
				}
				
				//queue it in a batch?
				if (isset($queryData['batch'])){
					return $query;
				}
				
				
				if ($this->_cache){
					$response=$this->_sdb->cache($this->_cache)->select($query,$options);
				} else {
					$response=$this->_sdb->select($query,$options);
				}

				$next_token = isset($response->body->SelectResult->NextToken)
					? (string) $response->body->SelectResult->NextToken
					: null;
		
				if ($response->isOK()){
					foreach ($response->body->SelectResult->Item as $item){
					$data=array();
					$data['id']=(string)$item->Name;
					$data['itemName']=(string)$item->Name;
					
					foreach ($item->Attribute as $attribute){
						if (isset($data[(string)$attribute->Name])){
							$data[(string)$attribute->Name]=array($data[(string)$attribute->Name]);
							$data[(string)$attribute->Name][]=(string)$attribute->Value;
						} else {
							$data[(string)$attribute->Name]=(string)$attribute->Value;
						}
					}
					
					$results[$i++][$model->alias]=$data;
					$found=true;
					}
				}
		} while ($next_token && (!empty($queryData['limit']) && count($results)<$queryData['limit'])); //TODO check limit condition
		$this->_registerLog($model, $query." cache=".$this->_cache, getMicrotime() - $startTime, count($results),$response->isOK());
	}
	//DEBUG($results);
	//DEBUG($fields);
	
	//ASSOCIATIONS
	$_associations = $model->__associations;
	if (!isset($queryData['recursive'])) {
			$queryData['recursive'] = $model->recursive;
		}
	if ($queryData['recursive'] > -1 && $fields!='count(*)') {
	
		$startTimeBatch = getMicrotime();
		foreach ($_associations as $type) {
			foreach ($model->{$type} as $assoc => $assocData) {
				$linkModel =& $model->{$assoc};
				$qdata=array('fields'=>$assocData['fields'],
							 'conditions'=>$assocData['conditions'],
							 'batch'=>1,
							 );
				
				$sdbBatch = new AmazonSDB();		
				
				//NO CACHE USING BATCH, the way cache ids are created for batches can produce very  bad results
				
				//override cache config with model config
				/*if (isset ($linkModel->cache_config)){
					$this->_cache_config=$linkModel->cache_config;
					$this->_cache=isset($linkModel->cache_time)?$linkModel->cache_time:'1 minute';
				}
				
				
				if ($this->_cache_config){
					$sdbBatch->set_cache_config($this->_cache_config);
					DEBUG($this->_cache_config." ".$this->_cache);
				}*/
				
				
				
				foreach($results as $key=>$value){
					$qdata['conditions'][$assoc.".".$assocData['foreignKey']]=$value[$model->alias][$model->primaryKey];
					$batchQuery[$key]=$this->read($linkModel,$qdata);
					//if ($this->_cache_config!=null){
						//$sdbBatch->batch()->cache($this->_cache)->select($batchQuery[$key]);
					//} else {
						$sdbBatch->batch()->select($batchQuery[$key]);
					//}
				}
				
				//get batch data
				//if ($this->_cache_config){
				//	$responses=$sdbBatch->batch()->cache($this->_cache)->send(false);
				//} else {
					$responses=$sdbBatch->batch()->send(false);
				//}
				
				//DEBUG($responses);
				//parse batch data
				
				$i=0;
				foreach ($responses as $response) {
					$batchCount=0;
					if ($response->isOK()){	
						$data=array();
						$data=$this->getDataFromResponse($linkModel,$response);
						$batchCount=count($data);
						$data = array($linkModel->alias=>Set::extract('{n}.' . $linkModel->alias, $data));
						$results[$i]=array_merge($results[$i],$data);
					} else {
						$results[$i]=array_merge($results[$i],array($linkModel->alias=>array()));
					}
					$this->_registerLog($linkModel, $batchQuery[$i]." cache=".$this->_cache." batch", getMicrotime() - $startTimeBatch, $batchCount,$response->isOK());
					$i++;
				}
				
				
				/*
				if ($model->useDbConfig == $linkModel->useDbConfig) {
					$db =& $this;
				} else {
					$db =& ConnectionManager::getDataSource($linkModel->useDbConfig);
				}

				if (isset($db)) {
					if (method_exists($db, 'queryAssociation')) {
						$stack = array($assoc);
						$db->queryAssociation($model, $linkModel, $type, $assoc, $assocData, $queryData, true, $results, $queryData['recursive'] - 1, $stack);
					}
					unset($db);
				}
				*/
			}
		}
	}
	//end associations
	
	if (trim($fields)=='count(*)'){
		if (!$primary_key){
			$count=$results[0][$model->alias]['Count'];
		} else {
			$count=count($results);
		}
		//DEBUG($count);
		return array(array(array('count' => $count)));
		}
	
	return $found?$results:null;
}

/**
 * get data from result
 */
function getDataFromResponse(&$model,&$response){
	$found=false;
	$results=array();
	$i=0;
	
	foreach ($response->body->SelectResult->Item as $item){
					$data=array();
					$data['id']=(string)$item->Name;
					
					foreach ($item->Attribute as $attribute){
						if (isset($data[(string)$attribute->Name])){
							$data[(string)$attribute->Name]=array($data[(string)$attribute->Name]);
							$data[(string)$attribute->Name][]=(string)$attribute->Value;
						} else {
							$data[(string)$attribute->Name]=(string)$attribute->Value;
						}
					}
					
					$results[$i++][$model->alias]=$data;
					$found=true;
					}
	return $found?$results:null;
} 
 
 /**
* Returns the field used for counts
*/
public function calculate(&$model, $func, $params = array()) {
return 'count(*)';
}

/**
* Save records
*/
public function create($model, $fields = array(), $values = array()) {
	$startTime=getMicrotime();
	//DEBUG("CREATE");
	//DEBUG($model);
	//DEBUG($values);
	//DEBUG(mktime());
	$isOK=false;
	$data = array_combine($fields, $values);
	
	$data['created']=mktime();
	//DEBUG($data);
	if (isset($data['id'])){
		$primary_key=$data['id'];
		unset($data['id']); //should we leave the id field?
		}
		else if ($model->id!=null){
		$primary_key=$model->id;
		}
		 else {
			$primary_key=String::uuid();
		 }
	
	//DEBUG($data);
	
	
	//DEBUG("put_attributes($model->useTable, $primary_key,".print_r($data,true).", true)");
	$response = $this->_sdb->put_attributes($model->useTable, $primary_key, $data, true);
	//DEBUG($response);
	$isOk=$response->isOK();
	
	if ($isOk){
	$model->setInsertId($primary_key);
	$model->id = $primary_key;
	}
	
	$this->_registerLog($model, "put_attributes(".$model->useTable.",".$primary_key.",".print_r($data,true).",true)" , getMicrotime() - $startTime, 0,$response->isOK());
	
	
return $isOk;
}	

/**
* delete a record
*/
function delete(&$model, $conditions = null) {
	$startTime=getMicrotime();
	
	
	if (!isset($conditions[$model->alias.".".$model->primaryKey])){
		return null;
	} else {
		$primary_key=$conditions[$model->alias.".".$model->primaryKey];
	}
	
	//DEBUG($conditions);
	//DEBUG("put_attributes($model->useTable, $primary_key,".print_r($data,true).", true)");
	$response = $this->_sdb->delete_attributes($model->useTable, $primary_key);
	
	$isOk=$response->isOK();
	
	$this->_registerLog($model, "delete_attributes(".$model->useTable.",".$primary_key.",null,true)" , getMicrotime() - $startTime, 0,$response->isOK());
	
	
return $isOk;
}


/**
 * update records
 */
public function update($model, $fields = array(), $values = array()) {
	
	$startTime=getMicrotime();
	
	//DEBUG($model);
	//DEBUG($fields);
	//DEBUG($values);
	//DEBUG(mktime());
	$isOK=false;
	$data = array_combine($fields, $values);
	//DEBUG($data);
	
	$data['modified']=mktime();
	if (isset($data['id'])){
		$primary_key=$data['id'];
		unset($data['id']);
		}
		else{
		$primary_key=$model->id;
		}
		
	//DEBUG($data);
	
	//DEBUG("put_attributes($model->useTable, $primary_key,".print_r($data,true).", true)");
	$response = $this->_sdb->put_attributes($model->useTable, $primary_key, $data, true);
	//DEBUG($response);
	
	$this->_registerLog($model, "put_attributes(".$model->useTable.",".$primary_key.",".print_r($data,true).",true)" , getMicrotime() - $startTime, 0,$response->isOK());
	
	$isOk=$response->isOK();
	$model->setInsertId($primary_key);
	
return $isOk;
}	

function name($alias){
return $alias;
}

/**
* Prepare a query
*/
public function query($method, $params, &$model){
$type=$params[0];

$field = Inflector::underscore(preg_replace('/^findBy/i', '', $method));

$querydata=array('conditions'=>array($field=>$params[1]));
if ($type==='first')
	{
	$querydata['limit']=1;
	}
return $this->read($model,$querydata);
}



/**
 * Get the query log as an array.
 *
 * @param boolean $sorted Get the queries sorted by time taken, defaults to false.
 * @param boolean $clear Clear after return logs
 * @return array Array of queries run as an array
 */
	public function getLog($sorted = false, $clear = true) {
		if ($sorted) {
			$log = sortByKey($this->_requestsLog, 'took', 'desc', SORT_NUMERIC);
		} else {
			$log = $this->_requestsLog;
		}
		if ($clear) {
			$this->_requestsLog = array();
		}
		return array('log' => $log, 'count' => count($log), 'time' => array_sum(Set::extract('{n}.took', $log)));
	}

/**
* Generate a log registry
*
* @param object $model
* @param array $queryData
* @param float $took
* @param integer $numRows
* @return void
*/
	public function _registerLog(&$model, $queryData, $took, $numRows,$isOK) {
		//DEBUG($queryData);
		//if (!Configure::read()) {
		//	return;
		//}
		$this->_requestsLog[] = array(
			'query' => $queryData,
			'error' => $isOK?'0':'1',
			'affected' => 0,
			'numRows' => $numRows,
			'took' => round($took, 3)
		);
	}

}
