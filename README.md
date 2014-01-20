Mongy
=====

The Mongy class represents a lightweight way to interact with MongoDB databases. Based on:
http://fuelphp.com/docs/classes/mongo/methods.html

## A Simple Example

```php
<?php

require_once('Mongy.php');

// Create a Mongy object
$m = Mongy::createMongyConnection(array(
    'hostname'   => 'localhost',
    'database'   => 'cooking'
));

// Create a object
$recipe = array(
    'name' => 'Spaghetti Bolognese Gran Gusto',
    'time' => '0:45 Minuten',
    'desc' => 'Zwiebeln in Scheiben schneiden, in eine Pfanne geben, salzen und in Öl gl...',
    'creationDate' => new MongoDate(),

    'ingredients' => array(
        array(
            '_id' => new MongoId(),
            'amount' => '20g',
            'name' => 'Zwiebel'
        ),
        array(
            '_id' => new MongoId(),
            'amount' => '500g',
            'name' => 'Hackfleisch'
        )
    )
);

// Insert a object
echo var_dump( $m->insert('recipes', $recipe) );

echo '<br>----<br>';
//

// Read collection
echo var_dump( $m->get('recipes') );

echo '<br>----<br>';
//

// Update only the name with a new object / array
$recipe2 = array(
    'name' => 'Spaghetti Bolognese Gran Gusto Sepcial'
);

$m->where(array('_id' => new MongoId($recipe['_id'])))->update('recipes', $recipe2, array(), false);
//

// Read one
echo var_dump( $m->where(array('_id' => new MongoId($recipe['_id'])))->getOne('recipes') );

echo '<br>----<br>';
//

// Delete all
$m->deleteAll('recipes');
//

// Read collection
echo var_dump( $m->get('recipes') );

echo '<br>----<br>';
//
```
