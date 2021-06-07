<?php

/**
 * [XinFox System] Copyright (c) 2011 - 2021 XINFOX.CN
 */
declare(strict_types=1);

namespace XinFox\Flysystem\AliYun;

class OssClient extends \OSS\OssClient
{
    protected $endpoint;

    public function __construct(
        $accessKeyId,
        $accessKeySecret,
        $endpoint,
        $isCName = false,
        $securityToken = null,
        $requestProxy = null
    ) {
        parent::__construct($accessKeyId, $accessKeySecret, $endpoint, $isCName, $securityToken, $requestProxy);

        $this->endpoint = $endpoint;
    }

    public function getEndpoint()
    {
        return $this->endpoint;
    }
}