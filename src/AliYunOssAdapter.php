<?php

/**
 * [XinFox System] Copyright (c) 2011 - 2021 XINFOX.CN
 */
declare(strict_types=1);

namespace XinFox\Flysystem\AliYun;

use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\Config;
use League\Flysystem\Util;

class AliYunOssAdapter extends AbstractAdapter
{
    protected OssClient $ossClient;

    protected string $bucket;

    protected array $options = [];

    public function __construct(OssClient $ossClient, string $bucket, $prefix = '', array $options = [])
    {
        $this->ossClient = $ossClient;
        $this->bucket = $bucket;
        $this->setPathPrefix(ltrim($prefix, '/'));
        $this->options = array_merge($this->options, $options);
    }

    public function write($path, $contents, Config $config)
    {
        $object = $this->applyPathPrefix($path);
        $options = $this->getOptionsFromConfig($config);

        if (!isset($options[OssClient::OSS_LENGTH])) {
            $options[OssClient::OSS_LENGTH] = Util::contentSize($contents);
        }

        if (!isset($options[OssClient::OSS_CONTENT_TYPE])) {
            $options[OssClient::OSS_CONTENT_TYPE] = Util::guessMimeType($path, $contents);
        }

        $this->ossClient->putObject($this->bucket, $object, $contents, $options);

        $type = 'file';
        $result = compact('type', 'path', 'contents');
        $result['mimetype'] = $options[OssClient::OSS_CONTENT_TYPE];
        $result['size'] = $options[OssClient::OSS_LENGTH];

        return $result;
    }

    /**
     * @throws \OSS\Core\OssException
     */
    public function writeStream($path, $resource, Config $config): bool
    {
        if (!is_resource($resource)) {
            return false;
        }
        $object = $this->applyPathPrefix($path);
        $i = 0;
        $bufferSize = 1000000; // 1M
        while (!feof($resource)) {
            if (false === $buffer = fread($resource, $block = $bufferSize)) {
                return false;
            }
            $position = $i * $bufferSize;
            $this->ossClient->appendObject(
                $this->bucket,
                $object,
                $buffer,
                $position,
                $this->getOptionsFromConfig($config)
            );
            $i++;
        }
        fclose($resource);
        return true;
    }

    public function update($path, $contents, Config $config)
    {
        $object = $this->applyPathPrefix($path);
        return $this->ossClient->putObject($this->bucket, $object, $contents, $this->getOptionsFromConfig($config));
    }

    public function updateStream($path, $resource, Config $config)
    {
        $object = $this->applyPathPrefix($path);
        $result = $this->write($object, stream_get_contents($resource), $config);
        if (is_resource($resource)) {
            fclose($resource);
        }
        return $result;
    }

    /**
     * @throws \OSS\Core\OssException
     */
    public function rename($path, $newpath)
    {
        $this->copy($path, $newpath);
        $this->delete($path);
    }

    /**
     * @throws \OSS\Core\OssException
     */
    public function copy($path, $newpath)
    {
        $object = $this->applyPathPrefix($path);
        $newObject = $this->applyPathPrefix($newpath);

        $this->ossClient->copyObject($this->bucket, $object, $this->bucket, $newObject);
    }

    public function delete($path)
    {
        $object = $this->applyPathPrefix($path);
        $this->ossClient->deleteObject($this->bucket, $object);
    }

    /**
     * @throws \OSS\Core\OssException
     */
    public function deleteDir($dirname)
    {
        $list = $this->listContents($dirname, true);

        $objects = [];
        foreach ($list as $val) {
            if ($val['type'] === 'file') {
                $objects[] = $this->applyPathPrefix($val['path']);
            } else {
                $objects[] = $this->applyPathPrefix($val['path']) . '/';
            }
        }

        $this->ossClient->deleteObjects($this->bucket, $objects);
    }

    public function createDir($dirname, Config $config)
    {
        $this->ossClient->createObjectDir($this->bucket, $dirname);
    }

    /**
     * @throws \OSS\Core\OssException
     */
    public function setVisibility($path, $visibility)
    {
        $object = $this->applyPathPrefix($path);
        $this->ossClient->putObjectAcl(
            $this->bucket,
            $object,
            ($visibility == 'public') ? 'public-read' : 'private'
        );
    }

    public function has($path)
    {
        $object = $this->applyPathPrefix($path);
        return $this->ossClient->doesObjectExist($this->bucket, $object);
    }

    /**
     * @throws \OSS\Core\OssException
     */
    public function read($path): array
    {
        return $this->getMetadata($path);
    }

    public function readStream($path): array
    {
        $object = $this->applyPathPrefix($path);
        $resource = "https://" . $this->bucket . '.' . $this->ossClient->getEndpoint()
            . '/' . ltrim($object, '/');
        return [
            'stream' => fopen($resource, 'r')
        ];
    }

    /**
     * @throws \OSS\Core\OssException
     */
    public function listContents($directory = '', $recursive = false): array
    {
        $directory = rtrim($this->applyPathPrefix($directory), '\\/');
        if ($directory) {
            $directory .= '/';
        }

        $bucket = $this->bucket;
        $delimiter = '/';
        $nextMarker = '';
        $options = [
            'delimiter' => $delimiter,
            'prefix' => $directory,
            'max-keys' => 1000,
            'marker' => $nextMarker,
        ];

        $listObjectInfo = $this->ossClient->listObjects($bucket, $options);

        $objectList = $listObjectInfo->getObjectList(); // 文件列表
        $prefixList = $listObjectInfo->getPrefixList(); // 目录列表

        $result = [];
        foreach ($objectList as $objectInfo) {
            if ($objectInfo->getSize() === 0 && $directory === $objectInfo->getKey()) {
                $result[] = [
                    'type' => 'dir',
                    'path' => $this->removePathPrefix(rtrim($objectInfo->getKey(), '/')),
                    'timestamp' => strtotime($objectInfo->getLastModified()),
                ];
                continue;
            }

            $result[] = [
                'type' => 'file',
                'path' => $this->removePathPrefix($objectInfo->getKey()),
                'timestamp' => strtotime($objectInfo->getLastModified()),
                'size' => $objectInfo->getSize(),
            ];
        }

        foreach ($prefixList as $prefixInfo) {
            if ($recursive) {
                $next = $this->listContents($this->removePathPrefix($prefixInfo->getPrefix()), $recursive);
                $result = array_merge($result, $next);
            } else {
                $result[] = [
                    'type' => 'dir',
                    'path' => $this->removePathPrefix(rtrim($prefixInfo->getPrefix(), '/')),
                    'timestamp' => 0,
                ];
            }
        }

        return $result;
    }

    /**
     * @param string $path
     * @return array
     * @throws \OSS\Core\OssException
     */
    public function getMetadata($path): array
    {
        $object = $this->applyPathPrefix($path);

        $result = $this->ossClient->getObjectMeta($this->bucket, $object);

        return [
            'type' => 'file',
            'dirname' => Util::dirname($path),
            'path' => $path,
            'timestamp' => strtotime($result['last-modified']),
            'mimetype' => $result['content-type'],
            'size' => $result['content-length'],
        ];
    }

    /**
     * @throws \OSS\Core\OssException
     */
    public function getSize($path): array
    {
        return $this->getMetadata($path);
    }

    /**
     * @throws \OSS\Core\OssException
     */
    public function getMimetype($path): array
    {
        return $this->getMetadata($path);
    }

    /**
     * @throws \OSS\Core\OssException
     */
    public function getTimestamp($path): array
    {
        return $this->getMetadata($path);
    }

    /**
     * @throws \OSS\Core\OssException
     */
    public function getVisibility($path): array
    {
        $response = $this->ossClient->getObjectAcl($this->bucket, $path);
        return [
            'visibility' => $response,
        ];
    }

    private function getOptionsFromConfig(Config $config): array
    {
        $options = [];
        if ($config->has("headers")) {
            $options['headers'] = $config->get("headers");
        }

        if ($config->has("Content-Type")) {
            $options["Content-Type"] = $config->get("Content-Type");
        }

        if ($config->has("Content-Md5")) {
            $options["Content-Md5"] = $config->get("Content-Md5");
            $options["checkmd5"] = false;
        }

        return $options;
    }
}