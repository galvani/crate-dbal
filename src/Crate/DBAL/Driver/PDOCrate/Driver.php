<?php
namespace Crate\DBAL\Driver\PDOCrate;

use Crate\DBAL\Platforms\CratePlatform4;
use Crate\DBAL\Schema\CrateSchemaManager;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver as DBALDriver;
use Doctrine\DBAL\Driver\API\ExceptionConverter;
use Doctrine\DBAL\Driver\Exception;
use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Query;

class Driver implements DBALDriver
{
    const VERSION = self::VERSION_4;
    const NAME = 'crate';

    private const VERSION_057 = '0.57.0';
    private const VERSION_4 = '4.0.0';

    /**
     * {@inheritDoc}
     * @return PDOConnection The database connection.
     */
    public function connect(array $params, $username = null, $password = null, array $driverOptions = array())
    {
        return new PDOConnection($this->constructPdoDsn($params), $username, $password, $driverOptions);
    }

    /**
     * Constructs the Crate PDO DSN.
     *
     * @return string The DSN.
     */
    private function constructPdoDsn(array $params)
    {
        $dsn = self::NAME . ':';
        if (isset($params['host']) && $params['host'] != '') {
            $dsn .= $params['host'];
        } else {
            $dsn .= 'localhost';
        }
        $dsn .= ':' . (isset($params['port']) ? $params['port'] : '4200');

        return $dsn;
    }

    public function getSchemaManager(Connection $conn, AbstractPlatform $platform)
    {
        return new CrateSchemaManager($conn, $this->getDatabasePlatform());
    }

    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return self::NAME;
    }

    public function getExceptionConverter(): ExceptionConverter
    {
        return new class implements ExceptionConverter {
            public function convert(Exception $exception, ?Query $query): DriverException
            {
                return new DriverException($exception,$query);
            }
        };
    }

    public function getDatabasePlatform()
    {
        return new CratePlatform4();
    }

}