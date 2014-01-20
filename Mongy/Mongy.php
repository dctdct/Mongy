<?php

/**
 * This class represents a custom Mongy exception.
 *
 * Class history:
 *  - 0.1: First release, working (David C. Thoemmes)
 *
 * @author David C. Thoemmes
 * @date 19.01.2013
 */
class MongyException extends Exception
{
    /**
     * Ctor. Call parent default ctor.
     *
     * @param string $message
     */
    public function __construct($message)
    {
        parent::__construct($message, 666);
    }

    /**
     * Overwrite toString method.
     *
     * @return string
     */
    public function __toString()
    {
        return __CLASS__ . ": [{$this->code}]: {$this->message}\n";
    }
}

/**
 * This code is based on Fuel framework MongoDB class. It is designed to use the class without Fuel.
 * Changed code convention to camelCase. Removed Fuel dependencies and added a few improvements.
 *
 * The Mongy class represents a lightweight way to interact with MongoDB databases.
 *
 * Class history:
 *  - 0.1: First release, working (David C. Thoemmes)
 *
 * @modified David C. Thoemmes <http://www.davidchristian.de>
 * @date 19.01.2013
 * @license http://www.opensource.org/licenses/mit-license.php The MIT License
 */

/**
 * This code is based on Redisent, a Redis interface for the modest.
 *
 * It has been modified to work with Fuel and to improve the code slightly.
 *
 * @author 		Justin Poliey <jdp34@njit.edu>
 * @copyright 	2009 Justin Poliey <jdp34@njit.edu>
 * @modified	Alex Bilbie
 * @modified	Phil Sturgeon
 * @license 	http://www.opensource.org/licenses/mit-license.php The MIT License
 */
class Mongy
{
    /** Holds the current mongo client reference. **/
    private $connection;

    /** Holds the current db reference. **/
    private $db;

    /** Holds all the select options. **/
    private $selects;

    /** Holds all the where options. **/
    private $wheres;

    /** Holds the sorting options. **/
    private $sorts;

    /** Holds the limit of the number of results to return. **/
    private $limit;

    /** The offset to start from. **/
    private $offset;

    /**
     * Ctor. Nothing todo.
     */
    private function __construct()
    {
        $this->reset();
    }

    /**
     * Drop a database.
     *
     * @param null $database
     * @return bool
     * @throws MongyException
     */
    public function dropDatabase($database = null)
    {
        if (empty($database))
        {
            throw new MongyException('Failed to drop MongoDB database because name is empty');
        }
        else
        {
            try
            {
                $this->connection->{$database}->drop();
                return true;
            }
            catch (Exception $e)
            {
                throw new MongyException("Unable to drop Mongo database `{$database}`: {$e->getMessage()}", $e->getCode());
            }
        }
    }

    /**
     * Drop a collection.
     *
     * @param string $database
     * @param string $col
     * @return bool
     * @throws MongyException
     */
    public function dropCollection($database = '', $col = '')
    {
        if (empty($database))
        {
            throw new MongyException('Failed to drop MongoDB collection because database name is empty');
        }

        if (empty($database))
        {
            throw new MongyException('Failed to drop MongoDB collection because collection name is empty');
        }
        else
        {
            try
            {
                $this->connection->{$database}->{$col}->drop();
                return true;
            }
            catch (Exception $e)
            {
                throw new MongyException("Unable to drop Mongo collection `{$col}`: {$e->getMessage()}", $e->getCode());
            }
        }
    }

    /**
     * Determine which fields to include OR which to exclude during the query process.
     * Currently, including and excluding at the same time is not available, so the
     * $includes array will take precedence over the $excludes array.  If you want to
     * only choose fields to exclude, leave $includes an empty array().
     *
     * @param array $includes
     * @param array $excludes
     * @return $this
     */
    public function select($includes = array(), $excludes = array())
    {
        if (!is_array($includes))
        {
            $includes = array($includes);
        }

        if (!is_array($excludes))
        {
            $excludes = array($excludes);
        }

        if (!empty($includes))
        {
            foreach ($includes as $col)
            {
                $this->selects[$col] = 1;
            }
        }
        else
        {
            foreach ($excludes as $col)
            {
                $this->selects[$col] = 0;
            }
        }

        return $this;
    }

    /**
     * Get the documents based on these search parameters. The $wheres array should
     * be an associative array with the field as the key and the value as the search
     * criteria.
     *
     * @param array $wheres
     * @return $this
     */
    public function where($wheres = array())
    {
        foreach ($wheres as $wh => $val)
        {
            $this->wheres[$wh] = $val;
        }

        return $this;
    }

    /**
     * Get the documents where the value of a $field may be something else.
     *
     * @param array $wheres
     * @return $this
     */
    public function orWhere($wheres = array())
    {
        if (count($wheres) > 0)
        {
            if (!isset($this->wheres['$or']) or !is_array($this->wheres['$or']))
            {
                $this->wheres['$or'] = array();
            }

            foreach ($wheres as $wh => $val)
            {
                $this->wheres['$or'][] = array($wh => $val);
            }
        }

        return $this;
    }

    /**
     * Get the documents where the value of a $field is in a given $in array().
     *
     * @param string $field
     * @param array $in
     * @return $this
     */
    public function whereIn($field = '', $in = array())
    {
        $this->whereInit($field);
        $this->wheres[$field]['$in'] = $in;

        return $this;
    }

    /**
     * Get the documents where the value of a $field is in all of a given $in array().
     *
     * @param string $field
     * @param array $in
     * @return $this
     */
    public function whereInAll($field = '', $in = array())
    {
        $this->whereInit($field);
        $this->wheres[$field]['$all'] = $in;

        return $this;
    }

    /**
     * Get the documents where the value of a $field is not in a given $in array().
     *
     * @param string $field
     * @param array $in
     * @return $this
     */
    public function whereNotIn($field = '', $in = array())
    {
        $this->whereInit($field);
        $this->wheres[$field]['$nin'] = $in;

        return $this;
    }

    /**
     * Get the documents where the value of a $field is greater than $x.
     *
     * @param string $field
     * @param $x
     * @return $this
     */
    public function whereGt($field = '', $x)
    {
        $this->whereInit($field);
        $this->wheres[$field]['$gt'] = $x;

        return $this;
    }

    /**
     * Get the documents where the value of a $field is greater than or equal to $x.
     *
     * @param string $field
     * @param $x
     * @return $this
     */
    public function whereGte($field = '', $x)
    {
        $this->whereInit($field);
        $this->wheres[$field]['$gte'] = $x;

        return $this;
    }

    /**
     * Get the documents where the value of a $field is less than $x.
     *
     * @param string $field
     * @param $x
     * @return $this
     */
    public function whereLt($field = '', $x)
    {
        $this->whereInit($field);
        $this->wheres[$field]['$lt'] = $x;

        return $this;
    }

    /**
     * Get the documents where the value of a $field is less than or equal to $x.
     *
     * @param string $field
     * @param $x
     * @return $this
     */
    public function whereLte($field = '', $x)
    {
        $this->whereInit($field);
        $this->wheres[$field]['$lte'] = $x;

        return $this;
    }

    /**
     * Get the documents where the value of a $field is between $x and $y.
     *
     * @param string $field
     * @param $x
     * @param $y
     * @return $this
     */
    public function whereBetween($field = '', $x, $y)
    {
        $this->whereInit($field);
        $this->wheres[$field]['$gte'] = $x;
        $this->wheres[$field]['$lte'] = $y;

        return $this;
    }

    /**
     * Get the documents where the value of a $field is between but not equal to $x and $y.
     *
     * @param string $field
     * @param $x
     * @param $y
     * @return $this
     */
    public function whereBetweenNe($field = '', $x, $y)
    {
        $this->whereInit($field);
        $this->wheres[$field]['$gt'] = $x;
        $this->wheres[$field]['$lt'] = $y;

        return $this;
    }

    /**
     * Get the documents where the value of a $field is not equal to $x.
     *
     * @param string $field
     * @param $x
     * @return $this
     */
    public function whereNe($field = '', $x)
    {
        $this->whereInit($field);
        $this->wheres[$field]['$ne'] = $x;

        return $this;
    }

    /**
     * Get the documents nearest to an array of coordinates (your collection must have a geospatial index).
     *
     * @param string $field
     * @param array $co
     * @return $this
     */
    public function whereNear($field = '', $co = array())
    {
        $this->whereInit($field);
        $this->wheres[$field]['$near'] = $co;

        return $this;
    }

    /**
     * Get the documents where the (string) value of a $field is like a value. The defaults
     * allow for a case-insensitive search.
     *
     * @param string $field
     * @param string $value
     *
     * @param string $flags
     * Allows for the typical regular expression flags:
     *		i = case insensitive
     *		m = multiline
     *		x = can contain comments
     *		l = locale
     *		s = dotall, "." matches everything, including newlines
     *		u = match unicode
     *
     * @param bool $enableStartWildcard
     * If set to anything other than TRUE, a starting line character "^" will be prepended
     * to the search value, representing only searching for a value at the start of
     * a new line.
     *
     * @param bool $enableEndWildcard
     * If set to anything other than TRUE, an ending line character "$" will be appended
     * to the search value, representing only searching for a value at the end of
     * a line.
     *
     * @return $this
     */
    public function like($field = '', $value = '', $flags = 'i', $enableStartWildcard = true, $enableEndWildcard = true)
    {
        $field = (string) trim($field);
        $this->whereInit($field);

        $value = (string) trim($value);
        $value = quotemeta($value);

        if ($enableStartWildcard !== true)
        {
            $value = '^' . $value;
        }

        if ($enableEndWildcard !== true)
        {
            $value .= '$';
        }

        $regex = "/$value/$flags";
        $this->wheres[$field] = new MongoRegex($regex);

        return $this;
    }

    /**
     * Sort the documents based on the parameters passed. To set values to descending order,
     * you must pass values of either -1, false, 'desc', or 'DESC', else they will be
     * set to 1 (ASC).
     *
     * @param array $fields
     * @return $this
     */
    public function orderBy($fields = array())
    {
        foreach ($fields as $col => $val)
        {
            if ($val == -1 or $val === false or strtolower($val) == 'desc')
            {
                $this->sorts[$col] = -1;
            }
            else
            {
                $this->sorts[$col] = 1;
            }
        }

        return $this;
    }

    /**
     * Limit the result set to $x number of documents.
     *
     * @param int $x
     * @return $this
     */
    public function limit($x = 99999)
    {
        if ($x !== null and is_numeric($x) and $x >= 1)
        {
            $this->limit = (int) $x;
        }

        return $this;
    }

    /**
     * Offset the result set to skip $x number of documents.
     *
     * @param int $x
     * @return $this
     */
    public function offset($x = 0)
    {
        if ($x !== null and is_numeric($x) and $x >= 1)
        {
            $this->offset = (int) $x;
        }

        return $this;
    }

    /**
     * Get the documents based upon the passed parameters.
     *
     * @param string $collection
     * @return mixed
     * @throws MongyException
     */
    public function getCursor($collection = '')
    {
        if (empty($collection))
        {
            throw new MongyException('In order to retrieve documents from MongoDB you must provide a collection name.');
        }

        $documents = $this->db->{$collection}->find($this->wheres, $this->selects)->limit((int) $this->limit)->skip((int) $this->offset)->sort($this->sorts);

        $this->reset();

        return $documents;
    }

    /**
     * Get the documents based upon the passed parameters.
     *
     * @param string $collection
     * @return array
     */
    public function get($collection = '')
    {
        $documents = $this->getCursor($collection);

        $returns = array();

        if ($documents and !empty($documents))
        {
            foreach ($documents as $doc)
            {
                $returns[] = $doc;
            }
        }

        return $returns;
    }

    /**
     * Get the document cursor from mongodb based upon the passed parameters.
     *
     * @param string $collection
     * @param array $where
     * @param int $limit
     * @return mixed
     */
    public function getWhere($collection = '', $where = array(), $limit = 99999)
    {
        return ($this->where($where)->limit($limit)->get($collection));
    }

    /**
     * Get one document based upon the passed parameters.
     *
     * @param string $collection
     * @return mixed
     * @throws MongyException
     */
    public function getOne($collection = '')
    {
        if (empty($collection))
        {
            throw new MongyException('In order to retrieve documents from MongoDB you must provide a collection name.');
        }

        $returns = $this->db->{$collection}->findOne($this->wheres, $this->selects);

        $this->reset();

        return $returns;
    }

    /**
     * Count the documents based upon the passed parameters.
     *
     * @param string $collection
     * @param bool $foundonly
     * @return mixed
     * @throws MongyException
     */
    public function count($collection = '', $foundonly = false)
    {
        if (empty($collection))
        {
            throw new MongyException('In order to retrieve documents from MongoDB you must provide a collection name.');
        }

        $count = $this->db->{$collection}->find($this->wheres)->limit((int) $this->limit)->skip((int) $this->offset)->count($foundonly);

        $this->reset();

        return ($count);
    }

    /**
     * Insert a new document into the passed collection.
     *
     * @param string $collection
     * @param array $insert
     * @return bool
     * @throws MongyException
     */
    public function insert($collection = '', $insert = array())
    {
        if (empty($collection))
        {
            throw new MongyException('In order to retrieve documents from MongoDB you must provide a collection name.');
        }

        if (empty($insert) or !is_array($insert))
        {
            throw new MongyException('Nothing to insert into Mongo collection or insert value is not an array');
        }

        try
        {
            $this->db->{$collection}->insert($insert, array('fsync' => true));

            if (isset($insert['_id']))
            {
                return $insert['_id'];
            }
            else
            {
                return false;
            }
        }
        catch (MongoCursorException $e)
        {
            throw new MongyException("Insert of data into MongoDB failed: {$e->getMessage()}", $e->getCode());
        }
    }

    /**
     * Updates a single document.
     *
     * @param string $collection
     * @param array $data
     * @param array $options
     * @param bool $literal
     * @return bool
     * @throws MongyException
     */
    public function update($collection = '', $data = array(), $options = array(), $literal = false)
    {
        if (empty($collection))
        {
            throw new MongyException('No Mongo collection selected to update');
        }

        if (empty($data) or !is_array($data))
        {
            throw new MongyException('Nothing to update in Mongo collection or update value is not an array');
        }

        try
        {
            $options = array_merge($options, array('fsync' => true, 'multiple' => false));

            $this->db->{$collection}->update($this->wheres, (($literal) ? $data : array('$set' => $data)), $options);

            $this->reset();

            return true;
        }
        catch (MongoCursorException $e)
        {
            throw new MongyException("Update of data into MongoDB failed: {$e->getMessage()}", $e->getCode());
        }
    }

    /**
     * Updates a collection of documents.
     *
     * @param string $collection
     * @param array $data
     * @param bool $literal
     * @return bool
     * @throws MongyException
     */
    public function updateAll($collection = "", $data = array(), $literal = false)
    {
        if (empty($collection))
        {
            throw new MongyException('No Mongo collection selected to update');
        }

        if (empty($data) or !is_array($data))
        {
            throw new MongyException('Nothing to update in Mongo collection or update value is not an array');
        }

        try
        {
            $this->db->{$collection}->update($this->wheres, (($literal) ? $data : array('$set' => $data)), array('fsync' => true, 'multiple' => true));

            $this->reset();

            return true;
        }
        catch (MongoCursorException $e)
        {
            throw new MongyException("Update of data into MongoDB failed: {$e->getMessage()}", $e->getCode());
        }
    }

    /**
     * Delete a document from the passed collection based upon certain criteria.
     *
     * @param string $collection
     * @return bool
     * @throws MongyException
     */
    public function delete($collection = '')
    {
        if (empty($collection))
        {
            throw new MongyException('No Mongo collection selected to delete from');
        }

        try
        {
            $this->db->{$collection}->remove($this->wheres, array('fsync' => true, 'justOne' => true));

            $this->reset();
            return true;
        }
        catch (MongoCursorException $e)
        {
            throw new MongyException("Delete of data into MongoDB failed: {$e->getMessage()}", $e->getCode());
        }
    }

    /**
     * Delete all documents from the passed collection based upon certain criteria.
     *
     * @param string $collection
     * @return bool
     * @throws MongyException
     */
    public function deleteAll($collection = '')
    {
        if (empty($collection))
        {
            throw new MongyException('No Mongo collection selected to delete from');
        }

        try
        {
            $this->db->{$collection}->remove($this->wheres, array('fsync' => true, 'justOne' => false));

            $this->reset();
            return true;
        }
        catch (MongoCursorException $e)
        {
            throw new MongyException("Delete of data from MongoDB failed: {$e->getMessage()}", $e->getCode());
        }
    }

    /**
     * Runs a MongoDB command (such as GeoNear). See the MongoDB documentation for more usage scenarios:
     * http://dochub.mongodb.org/core/commands
     *
     * @param array $query
     * @return mixed
     * @throws MongyException
     */
    public function command($query = array())
    {
        try
        {
            $run = $this->db->command($query);
            return $run;
        }

        catch (MongoCursorException $e)
        {
            throw new MongyException("MongoDB command failed to execute: {$e->getMessage()}", $e->getCode());
        }
    }

    /**
     * Ensure an index of the keys in a collection with optional parameters. To set values to descending order,
     * you must pass values of either -1, false, 'desc', or 'DESC', else they will be
     * set to 1 (ASC).
     *
     * @param string $collection
     * @param array $keys
     * @param array $options
     * @return $this
     * @throws MongyException
     */
    public function addIndex($collection = '', $keys = array(), $options = array())
    {
        if (empty($collection))
        {
            throw new MongyException('No Mongo collection specified to add an index to');
        }

        if (empty($keys) or !is_array($keys))
        {
            throw new MongyException('Index could not be created to MongoDB Collection because no keys were specified');
        }

        foreach ($keys as $col => $val)
        {
            if($val == -1 or $val === false or strtolower($val) == 'desc')
            {
                $keys[$col] = -1;
            }
            else
            {
                $keys[$col] = 1;
            }
        }

        if ($this->db->{$collection}->ensureIndex($keys, $options) == true)
        {
            $this->reset();
            return $this;
        }
        else
        {
            throw new MongyException('An error occured when trying to add an index to MongoDB Collection');
        }
    }

    /**
     * Remove an index of the keys in a collection. To set values to descending order,
     * you must pass values of either -1, false, 'desc', or 'DESC', else they will be
     * set to 1 (ASC).
     *
     * @param string $collection
     * @param array $keys
     * @return $this
     * @throws MongyException
     */
    public function removeIndex($collection = '', $keys = array())
    {
        if (empty($collection))
        {
            throw new MongyException('No Mongo collection specified to remove an index from');
        }

        if (empty($keys) or !is_array($keys))
        {
            throw new MongyException('Index could not be removed from MongoDB Collection because no keys were specified');
        }

        if ($this->db->{$collection}->deleteIndex($keys) == true)
        {
            $this->reset();
            return $this;
        }
        else
        {
            throw new MongyException('An error occured when trying to remove an index from MongoDB Collection');
        }
    }

    /**
     * Remove all indexes from a collection.
     *
     * @param string $collection
     * @return $this
     * @throws MongyException
     */
    public function removeAllIndexes($collection = '')
    {
        if (empty($collection))
        {
            throw new MongyException('No Mongo collection specified to remove all indexes from');
        }

        $this->db->{$collection}->deleteIndexes();
        $this->reset();

        return $this;
    }

    /**
     * Lists all indexes in a collection.
     *
     * @param string $collection
     * @return mixed
     * @throws MongyException
     */
    public function listIndexes($collection = '')
    {
        if (empty($collection))
        {
            throw new MongyException('No Mongo collection specified to remove all indexes from');
        }

        return ($this->db->{$collection}->getIndexInfo());
    }

    /**
     * Returns a collection object so you can perform advanced queries, upserts, pushes and addtosets.
     *
     * @param $collection
     * @return mixed
     */
    public function getCollection($collection)
    {
        return ($this->db->{$collection});
    }

    /**
     * Close the connection.
     */
    public function close()
    {
        $this->connection->close();
    }

    /**
     * Reset every variable. Called after get() and other methods.
     */
    public function reset()
    {
        $this->selects = array();
        $this->wheres = array();
        $this->limit = 999999;
        $this->offset = 0;
        $this->sorts = array();
    }

    /**
     * Prepares parameters for insertion in $wheres array().
     * @param $param
     */
    private function whereInit($param)
    {
        if (!isset($this->wheres[$param]))
        {
            $this->wheres[$param] = array();
        }
    }

    /**
     * Create a mongy object. Pass a config object with host, pass and user.
     *
     * @param array $config
     * @return Mongy
     * @throws MongyException
     */
    public static function createMongyConnection(array $config = array())
    {
        // Build up a connect options array for mongo
        $options = array('connect' => true);

        if (!empty($config['replicaset']))
        {
            $options['replicaSet'] = $config['replicaset'];
        }

        $connectionString = "mongodb://";

        if (empty($config['hostname']))
        {
            throw new MongyException('The host must be set to connect to MongoDB');
        }

        if (empty($config['database']))
        {
            throw new MongyException('The database must be set to connect to MongoDB');
        }

        if (!empty($config['username']) and ! empty($config['password']))
        {
            $connectionString .= "{$config['username']}:{$config['password']}@";
        }

        if (isset($config['port']) and ! empty($config['port']))
        {
            $connectionString .= "{$config['hostname']}:{$config['port']}";
        }
        else
        {
            $connectionString .= "{$config['hostname']}";
        }

        $connectionString .= "/{$config['database']}";

        // Create mongo client
        try
        {
            $mongy = new Mongy();
            $mongy->connection = new MongoClient(trim($connectionString), $options);

            $mongy->db = $mongy->connection->{$config['database']};

            return $mongy;
        }
        catch (MongoConnectionException $e)
        {
            throw new MongyException("Unable to connect to MongoDB: {$e->getMessage()}", $e->getCode());
        }
    }
}